
/*
 *
 *     Copyright (c) 2019 Sandeep Virdi
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package com.virdis.bloom

import java.nio.{ByteBuffer, ByteOrder}

import com.virdis.models.GeneratedKey
import com.virdis.utils.{Constants}
import net.jpountz.xxhash.XXHashFactory

sealed abstract case class BloomFilter(noOfBits: Int, noOfHashes: Int) {
  final val hasher = XXHashFactory.fastestInstance().hash64()
  final val byteBuffer = ByteBuffer.allocateDirect(noOfBits)

  def add(generatedKey: GeneratedKey): Unit = {
    val hash = hasher.hash(generatedKey.toBuffer, BloomFilter.BF_SEED)
    val bb = ByteBuffer.allocate(Constants.LONG_SIZE_IN_BYTES).order(ByteOrder.BIG_ENDIAN)
    bb.putLong(hash)
    bb.flip()
    val hash2 = hasher.hash(bb, BloomFilter.BF_SEED)
    var i = 0
    while(i < noOfHashes) {
      val bitToSet: Long = ((hash + (i * hash2)) & Long.MaxValue) % noOfBits
      byteBuffer.put(bitToSet.toInt, Constants.TRUE_BYTES)
      i += 1
    }
  }

  def contains(generatedKey: GeneratedKey): Boolean = {
    val hash = hasher.hash(generatedKey.toBuffer, BloomFilter.BF_SEED)
    val bb = ByteBuffer.allocate(Constants.LONG_SIZE_IN_BYTES).order(ByteOrder.BIG_ENDIAN)
    bb.putLong(hash)
    bb.flip()
    val hash2 = hasher.hash(bb, BloomFilter.BF_SEED)
    var i = 0
    var result = 1
    while(i < noOfHashes) {
      val bitToCheck: Long = ((hash + (i * hash2)) & Long.MaxValue) % noOfBits
      val bitValue: Byte = byteBuffer.get(bitToCheck.toInt)
      result = result & bitValue
      i += 1
    }
    result == 1
  }

}

object BloomFilter {
  final val BF_SEED = 0x9747b49a
}