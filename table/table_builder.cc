// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;          // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

/*
 *                 Raw Block
 *
 *           |-------------------|
 *           |      Entry 1      |  Entry1, Entry2... 是按照字典序进
 *           |-------------------|  行排列的, 其中含有shared,
 *           |      Entry 2      |  non_shared, value_size... 等等一
 *           |-------------------|  些字段，实际上存储的就是KV键值对,
 *           |      Entry 3      |  详细可看block_builder.cc的Add()方
 *           |-------------------|  法上有注释.
 *           |      Entry 4      |
 *           |-------------------|
 *           |        ...        |
 *           |-------------------|
 *  4 Bytes  |    restarts[0]    |  leveldb中每隔固定条数的Entry会强
 *           |-------------------|  制加入一个重启点, 这里存储的数组
 *  4 Bytes  |    restarts[2]    |  restarts实际上就是指向这些重启点
 *           |-------------------|  的.
 *  4 Bytes  |    restarts[3]    |
 *           |-------------------|
 *  4 Bytes  |         3         |  重启点数组的大小
 *           |-------------------|
 *  1 Byte   |  CompressionType  |  数据的压缩方式, 是kSnappy或者kNo
 *           |-------------------|
 *  4 Bytes  |        CRC        |  根据上方除了CompressionType计算出来的一个校验值
 *           |-------------------|
 *
 *  上图是一个完整的Raw Block.
 *
 *  对于Index Block: 每当写完一个完整的Raw Block都会计算出一个索引key(大
 *  于或者等于当前Block中最大Key的那个Key), 以及当前Raw Block在文件中距
 *  离文件起始位置的偏移量以及当前Data Block(在这里Raw Block去掉CompressionType
 *  和CRC我们称为Data Block)的大小, 我们会将索引key, 当前data block的偏移量
 *  以及当前data block的大小当做一条Entry写入到Index Block当中.
 *
 *  对于Filter Block: 我们会根据当前这个Data Block添加的所有Key计算出一个
 *  字符串str追加到result_后面(以后当我们给出一个key我们可以根据这个str
 *  快速的查出当前Block中是否存在这个key了), 然后我们还会将str在result_中
 *  的起始位置添加到filter_offsets_当中.
 */


void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // 当我们写完一个Raw Block以后要记录下一些数据
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    // 计算出大于或者等于当前Block中最大Key的那个Key(我们称为索引Key)
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    // 记录当前Raw Block在文件中的位置距离文件起始位置的偏移量
    // 记录当前block_content的size (注意，这里不是整个Raw Block的
    // 大小而是KV数据和restarts_这些数据的大小, 我们称为Data Block)
    r->pending_handle.EncodeTo(&handle_encoding);
    // 将我们计算出来的当前Block的索引Key, 以及当前Block距离文件起始
    // 位置的偏移量和Block大小添加到index_block当中(实际上index_block
    // 的结构和Data Block是一模一样的, 就是会有重启点, 和共享前缀这些)
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    // 将其置为false, 表示当前Raw Block已经处理完毕
    r->pending_index_entry = false;
  }

  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

/*
 * 这个方法是将当前的数据Block转换成RawBlock写入到文件当中, 做的事情也比较简单
 * 就是在根据Data Block算出CRC, 然后在Data Block后面追加上CompressionType和CRC
 * 这便是RawBlock, 我们将这个RawBlock追加到文件的尾部，然后更新文件偏移量。
 *
 * ******************************** Raw Block Format ********************************
 *
 * |   <Data Block>   |   <Type>   |   <CRC>   |
 *       * Bytes         1 Bytes      4 Bytes
 *
 * 这里的<Block>的存储的是用户的KV数据，然后Block的尾部存储重启点集合和重启点集合大小
 * 详细可以参照block_builder.cc里的Add()方法上有详细的注释.
 * 这里的<Type>占用1 Bytes, 记录的是数据的压缩方案目前支持两种kNoCompression和kSnappyCompression.
 * 最后的<CRC>占用4 Bytes, 记录的是数据校验码，是根据block_contents生成的一个uint32_t类型的整数,
 * 用于判别数据是否在生成和传输中出错.
 *
 */

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

/*
 *   下图就是整个Table在文件中的物理布局形式, 我们读取到该文件之后首先从最后48字节中
 *   读取到该Table的Footer.
 *   通过Footer中我们可以获取到IndexBlock在文件中的起始位置和该Block的大小, 通过解析
 *   Index Block, 我们可以获取到文件中每个Raw Block在文件中的位置信息和每个Raw Block
 *   的索引Key, 这样就可以快速的定位到我们当前需要查询的Key可能存在于哪个Raw Block当
 *   中.
 *   通过Footer我们还可以获取到MetaIndex Block, MetaIndex Block中含有Filter Block在
 *   文件中的起始位置和该Block的大小, 我们通过解析Filter Block可以获取到对应于每段
 *   Raw Block的布隆过滤器的关键字符串, 通过这个字符串我们可以判断对应Raw Block是否
 *   存在我们需要查询的Key.
 *   至此, 一个Table建立的过程以及最后Table的结构已经完全介绍完毕.
 *
 *            |-------------------|---
 *            |    Raw Block 1    |   \
 *            |-------------------|    |
 *            |    Raw Block 2    |    |
 *            |-------------------|      --> Raw Block中存储了真实的数据以及重启点等信息
 *            |    Raw Block 3    |    |     具体可以看本文件中TableBuilder::Add()方法上
 *            |-------------------|    |     方有注释
 *            |        ...        |   /
 *            |-------------------|---
 *            |      result_      |   \
 *            |-------------------|    |
 *  4 Bytes   |    offsets_[0]    |    |
 *            |-------------------|    |
 *  4 Bytes   |    offsets_[1]    |    |
 *            |-------------------|    |
 *  4 Bytes   |    offsets_[2]    |    |
 *            |-------------------|      --> 这部分是Filter Block, 其中result_是由各个Data Block中的Key通过Hash计算出来的特征字符串拼接
 *            |        ...        |    |     来的, 下面有一个offset_数组, offset_[0]记录result_中属于Data Block 1的特征字符串的起始位置,
 *            |-------------------|    |     last_word记录的是result_字符串的大小
 *  4 Bytes   |    last_word      |    |
 *            |-------------------|    |
 *  1 Byte    |  kNoCompression   |    |
 *            |-------------------|    |
 *  4 Bytes   |        CRC        |   /
 *            |-------------------|---
 *            |  MetaIndex Block  |      --> 用于记录Filter的名称，以及上方Filter Block的起始位置和大小(该Block尾部也包含压缩方式和CRC)
 *            |-------------------|---
 *            |  Pending Entry 1  |   \
 *            |-------------------|    |
 *            |  Pending Entry 2  |    |
 *            |-------------------|    |
 *            |  Pending Entry 3  |    |
 *            |-------------------|      --> 这部分是Index Block, 其中的Pending Entry 1中包含Raw Block 1的索引Key, 以及Raw Block 1在文件
 *            |        ...        |    |     中的位置信息, 以及Raw Block 1的大小.
 *            |-------------------|    |
 *  1 Bytes   |  kNoCompression   |    |
 *            |-------------------|    |
 *  4 Bytes   |        CRC        |   /
 *            |-------------------|---
 *  48 Bytes  |       Footer      |      --> 包含MetaIndex Block的Index Block的索引信息(在文件中的位置以及大小), 和魔数, 具体细节可以看format.cc的Footer::EncodeTo();
 *            |-------------------|
 *
 *  其中RawBlock和MetaIndexBlock还有IndexBlock内部的结构都是一样的，将每个KV编码成一个条目之后添加到Block当中
 *  然后如果前一个条目和当前条目有前缀重叠部分, 那么就会通过共享前缀来节约空间, 每隔固定的条目该Block都会强制
 *  加入一个重启点, 该Block的尾部会加入重启点数组, 以及重启点数组的大小
 */

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  // BlockHandle的作用是记录各个Block在文件中的位置以及
  // 各个Block的大小, 并且其中有方法将这个两个数据编码为
  // 一个字符串
  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb
