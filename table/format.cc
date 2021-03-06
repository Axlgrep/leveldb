// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

/*
 *  用来编码文件的Footer, Footer包含Meta Block
 *  在文件中的位置以及Meta Block的大小以及
 *  Index Block在文件中的位置和Index Block的大
 *  小以及文件尾部的"魔数"
 *
 *  Q: 为什么要添加Padding?
 *  A: 由于这里记录这些Block的位置以及大小是使用
 *     的紧凑型的数字表示法, 所以表示一个int64_t
 *     的数字使用的空间在1 ~ 10个Bytes不等, 为了
 *     我们从尾部能快速的找到Footer的在文件中的起
 *     始位置(8 + 4 * 10)Bytes, 所以我们在这里加
 *     入了Padding进行填充
 *
 *                  |---------------------|
 *   1 ~ 10 Bytes   |  Meta Block offset  |
 *                  |---------------------|
 *   1 ~ 10 Bytes   |   Meta Block size   |
 *                  |---------------------|
 *   1 ~ 10 Bytes   | Index Block offset  |
 *                  |---------------------|
 *   1 ~ 10 Bytes   |  Index Block size   |
 *                  |---------------------|
 *                  |       Padding       |
 *                  |---------------------|
 *     8 Bytes      |     MagicNumber     |
 *                  |---------------------|
 */

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

/*
 * 我们输入一段Footer数据， 然后我们解析之后先进行魔术匹配
 * 解析出Meta Block在文件中的offset和size
 * 然后解析出Index Block在文件中的offset和size
 */
Status Footer::DecodeFrom(Slice* input) {
  //Footer的最后8位为魔术, 首先进行魔数的匹配
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  // BlockHandle只会记录每个Block自身的大小，后面的1 Bytes的压缩类型
  // 和4 Bytes CRC 并不会记录在内
  //
  // |    <Block>    |   <Type>   |   <CRC>   |
  //   handle.size()    1 Bytes      4 Bytes

  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];
  Slice contents;
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // 如果需要CRC的校验就进行校验
  // Check the crc of the type and the block contents
  const char* data = contents.data();    // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      // 如果读取数据的长度小于RandomAccessFile中一个Block的大小，
      // 那么contents直接指向RandomAccess文件中的block引用。
      // 如果读取的数据长度大于RandomAccessFile中一个Block的大小，
      // 那么就需要将多个Block中的数据拷贝到我们自己创建的buffer空间
      // 当中，而这部分空间是在heap上进行分配的，后续我们需要手动
      // 释放，所以在下面会做标记
      if (data != buf) {
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } else {
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: {
      // 如果采用Snappy形式进行压缩，首先先获取数据压缩之前的实际长度
      // 是多少，然后分配对应的空间存储解压之后的数据
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace leveldb
