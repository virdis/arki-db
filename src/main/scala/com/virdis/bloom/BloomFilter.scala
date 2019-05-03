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
import com.virdis.utils.{Config, Constants}
import net.jpountz.xxhash.{XXHash64, XXHashFactory}
import scodec.bits.{BitVector, ByteOrdering}

// https://stackoverflow.com/questions/48727174/bloom-filters-and-its-multiple-hash-functions
final class BloomFilter(val bits: Int, val hashes: Int) {
  // make these configurable
  val hasher: XXHash64 = XXHashFactory.fastestInstance().hash64()

  final def add(bv: BitVector, generatedKey: GeneratedKey): BitVector = {
    val hash1 = hasher.hash(
      ByteBuffer.wrap(BitVector.fromLong(generatedKey.underlying, 64, ByteOrdering.BigEndian).toByteArray),
      Constants.BLOOM_SEED)
    val hash2 = hasher.hash(
      ByteBuffer.wrap(BitVector.fromLong(hash1, 64, ByteOrdering.BigEndian).toByteArray),
      Constants.BLOOM_SEED)
    var i = 0
    var bitVector = bv
    while (i < hashes) {
      val bitToSet = ((hash1 + (i * hash2)) & Long.MaxValue) % bits
      bitVector = bitVector.set(bitToSet)
      i += 1
    }
    bitVector
  }

  final def contains(bv: BitVector, generatedKey: GeneratedKey): Boolean = {
    val hash1 = hasher.hash(generatedKey.toBuffer, Constants.BLOOM_SEED)
    val bb = ByteBuffer.allocate(Constants.LONG_SIZE_IN_BYTES).order(ByteOrder.BIG_ENDIAN)
    bb.putLong(hash1)
    bb.flip()
    val hash2 = hasher.hash(bb, Constants.BLOOM_SEED)
    var i = 0
    var result = true
    while (i < hashes) {
      val bitToCheck: Long = ((hash1 + (i * hash2)) & Long.MaxValue) % bits
      val bitValue = bv.get(bitToCheck)
      result = result && bitValue // maybe return early
      i += 1
    }
    result
  }
}