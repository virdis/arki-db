
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

import java.nio.ByteBuffer
import java.util.Random

import com.virdis.BaseSpec
import com.virdis.models.GeneratedKey
import com.virdis.utils.Constants
import net.jpountz.xxhash.{XXHash64, XXHashFactory}
import org.scalacheck.Gen
import scodec.bits.{BitVector, ByteOrdering}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

class BloomFilterSpec extends BaseSpec {

  val hasher: XXHash64 = XXHashFactory.fastestInstance().hash64()
  val bits = 6155
  val hashes = 5
  /**
    * n = 5000
    * p = 0.01 (1 in 100)
    * m = 49245 (6.01KiB)
    * k = 5
    */
  class Fixture {

    val bf = new BloomFilter(bits, hashes)
    val random = new Random()
  }
  it should "add should set bits to 1" in {
    val f = new Fixture
    import f._
    var bitVector = BitVector.fill(bf.bits)(false)
    val list = List.fill(5000)(random.nextLong())
    val idxList: List[ListBuffer[Long]] = list.map(getIndices)
    list.foreach(l => bitVector = bf.add(bitVector, GeneratedKey(l)))
    val bools = list.map(l => bf.contains(bitVector, GeneratedKey(l)))
    Future{ assert(bools.fold(true)(_ && _)) }


  }

  def getIndices(k: Long) = {
    val bv = BitVector.fromLong(k, 64, ByteOrdering.BigEndian)
    val hash1 = hasher.hash(ByteBuffer.wrap(bv.toByteArray), Constants.BLOOM_SEED)
    val bv2 = BitVector.fromLong(hash1, 64, ByteOrdering.BigEndian)
    val hash2 = hasher.hash(ByteBuffer.wrap(bv.toByteArray), Constants.BLOOM_SEED)
    var i = 0
    var listBuffer = new scala.collection.mutable.ListBuffer[Long]
    while(i < 5) {
      val index = ((hash1 + (i * hash2)) & Long.MaxValue) % bits
      listBuffer.append(index)
      i+=1
    }
    listBuffer
  }
}
