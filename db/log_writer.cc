// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() {
}

/*
 * 这方法是在在log文件中添加一条Record，leveldb对于一个log文件，会把它切割成
 * 32k为单位的物理block, 每次读取以一个block作为基本单位, 一个log文件就是由
 * 一系列连续的block组成的
 *
 * log文件中一条记录的格式是这样的:
 *
 * **************************** format ****************************
 * | <CheckSum> | <Record Length> | <Record Type> |   Record Content   |
 *    4 Bytes         2 Bytes          1 Bytes
 *
 * CheckSum: 根据当前Record Content计算出来的一个
 * 校验值(注意一条Record如果分到了多个block中去存储，那么每条记录
 * 的CheckSum是不一样的，因为是根据Record的子串进行计算的)
 * Record Length: 记录着Record Content的长度
 * Record Type: 当前这条记录的类型(后面会细说)
 * Record Content: Record中的内容
 * 在这里我们将CheckSum, Record Length, Record Type称为记录的Header
 * Header的大小是4 + 2 + 1 = 7 Bytes
 *
 * 其中Record Type有常用的有四种类型:
 * kFullType, kFirstType, kMiddleType, kLastType,
 *
 * kFullType:
 * 当我们当前这个block剩余的可用空间能存储下当前这条Record的全部
 * 内容以及记录的Header，当前记录的Record Type就是kFullType
 *
 * e.g..
 * 在一个全新的block里面可用剩余空间为32kb = 32768 Bytes, 我们有一个
 * 长度为20kb(20480 Bytes)的Record, 下面记录了该条记录在Block中的表现形式
 *
 * | <CheckSum> | <Record Length> | <Record Type> |   Record Content   |
 *     ****           20480           kFullType          content
 *
 * 当前Block的第0 ~ 3 Bytes存储这条Record的CheckSum
 * 当前Block的第4 ~ 5 Bytes存储这条Record的长度，也就是20480
 * 当前Block的第6 Bytes存储这条记录的类型，也就是kFullType
 * 当前Block的第7 ~ 20486 Bytes存储这条Record的内容
 * 然后当前这个block还有32768 - 20487 = 12281 Bytes的可用剩余空间
 *
 *
 * kFirstType:
 * 当我们当前这个block剩余可用空间只能存储Record的部分内容以及
 * 记录的Header，当前记录的Record Type就是kFirstType
 *
 * kMiddleType:
 * 当我们的一条Record在前一个Block存储了部分内容，并且还有剩余内容需要
 * 在当前Block中存储，并且用尽当前Block全部空间(32kb)还是不足以存储Record
 * 的剩余内容以及记录的Header，当前记录的Record Type就是kMiddleType
 *
 * kLastType:
 * 当我们的一条Record在之前的Block存储了部分内容，并且还有剩余内容需要
 * 在当前Block中存储，并且当前Block全部空间(32kb)足以存储Record的剩余内容
 * 以及记录的Header，当前记录的Record Type就是kLastType
 *
 * e.g..
 * 我们有一个长度为80kb(81920 Bytes)的Record, 这条Record 0 ~ 32761 Byte存储
 * 字符s, 32762 ~ 65521 Byte存储字符m, 65522 ~ 81919存储字符l
 *
 * 存储下这条Record我们需要用到三个连续的Block,
 * 下面记录了该条记录在三个连续Block中的表现形式
 *
 * | <CheckSum> | <Record Length> | <Record Type> |    Record Content...   |
 *     ****           32761           kFirstType            sss...
 * | <CheckSum> | <Record Length> | <Record Type> |    Record Content...   |
 *     ****           32761           kMiddleType           mmm...
 * | <CheckSum> | <Record Length> | <Record Type> |    Record Content...   |
 *     ****           16398           kLastType             lll...
 *
 * 第一个Block的第0 ~ 3 Bytes存储根据子串sss...计算出来的CheckSum
 * 第一个Block的第4 ~ 5 Bytes存储Record开头一段字符串的长度，在这里是32761
 * 第一个Block的第6 Bytes存储这条记录的类型，也就是kFirstType
 * 第一个Block的第7 ~ 32767 Bytes存储这段字符串的内容
 * 至此第一个block已经充分利用完毕, Record还剩81920 - 32761 = 49159 Bytes没有存储
 *
 * 由于使用了前面Block但是该条Record还是没有存储完毕，我们再用一个Block进行存储
 * 第二个Block的第0 ~ 3 Bytes存储根据子串mmm...计算出来的CheckSum
 * 第二个Block的第4 ~ 5 Bytes存储Record中间一段字符串的长度，在这里是32761
 * 第二个Block的第6 Bytes存储这条记录的类型，也就是kMiddleType
 * 第二个Block的第7 ~ 32767 Bytes存储这段字符串的内容
 * 至此第二个block已经充分利用完毕, Record还剩49159 - 32761 = 16398 Bytes没有存储
 *
 * 由于使用了前面block但是该条Record还剩16398 Bytes没有存储完毕，我们再利
 * 用最后一个Block将剩余部分存储完毕即可
 * 第三个Block的第0 ~ 3 Bytes存储根据子串lll...计算出来的CheckSum
 * 第三个Block的第4 ~ 5 Bytes存储Record末尾一段字符串的长度，在这里是16398
 * 第三个Block的第6 Bytes存储这条记录的类型，也就是kMiddleType
 * 第三个Block的第7 ~ 16404 Bytes存储这段字符串的内容
 * 至此这个Record已经全部存储完毕，第三个Block还剩32768 - 16405 = 16363
 * Bytes的可用空间
 *
 */
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        // 若当前Block中剩余可用空间已经不足以存储一条记录的Header，则剩余的
        // 空间利用'\x00'进行填充
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  // kHeaderSize的值为7
  // 0 ~ 3 记录该条记录的CheckSum
  // 4 ~ 5 记录该条记录的长度
  // 6 记录该条记录的类型, kFullType or kFirstType or kLastType or kMiddleType
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  // Extend方法实际上是根据该条记录计算出一个CheckSum, 就不深入看了
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + n;
  return s;
}

}  // namespace log
}  // namespace leveldb
