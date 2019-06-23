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

import com.virdis.hashing.Hasher
import com.virdis.models.GeneratedKey
import scodec.bits.{BitVector, ByteOrdering, ByteVector}

// https://stackoverflow.com/questions/48727173/bloom-filters-and-its-multiple-hash-functions

trait BloomFilter {
  final val hasher = Hasher.xxhash64
  def bits: Int
  def hashes: Int
  val bloomfilter: Array[Int] = new Array[Int](bits)

  @inline final def genHashes(generatedKey: GeneratedKey): (GeneratedKey, GeneratedKey) = {
    val hash1 = hasher.hash(BitVector.fromLong(generatedKey.underlying, 64, ByteOrdering.BigEndian).toByteArray)
    val hash2 = hasher.hash(BitVector.fromLong(hash1.underlying, 64, ByteOrdering.BigEndian).toByteArray)
    (hash1, hash2)
  }
  // using GeneratedKey value class for hashes, refactor
  @inline final def getBit(hash1: GeneratedKey, hash2: GeneratedKey, idx: Int): Long = {
    ((hash1.underlying + (idx * hash2.underlying)) & Long.MaxValue) % bits
  }

  @inline final def put(generatedKey: GeneratedKey) = {
    val (hash1, hash2) = genHashes(generatedKey)
    var i = 0
    while (i < hashes) {
      val bitToSet = getBit(hash1, hash2, i)
      bloomfilter(bitToSet.toInt) = 1
      i += 1
    }
  }

  @inline final def contains(generatedKey: GeneratedKey): Boolean = {
    val (hash1, hash2) = genHashes(generatedKey)
    var i = 0
    var result = true
    while (i < hashes) {
      val bitToCheck: Long = getBit(hash1, hash2, i)
      val bitValue = if (bloomfilter(bitToCheck.toInt) == 1) true else false
      result = result && bitValue // maybe return early
      i += 1
    }
    result
  }
}

final class BloomFilterF(override val bits: Int,
                         override val hashes: Int) extends BloomFilter

