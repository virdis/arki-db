
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

import java.util.Random

import com.virdis.BaseSpec
import com.virdis.models.GeneratedKey
import net.jpountz.xxhash.{XXHash64, XXHashFactory}

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

    val bf = new BloomFilterF(bits, hashes)
    val random = new Random()
  }
  it should "return true if element is present" in {
    val f = new Fixture
    import f._
    val num1 = random.nextLong()
    bf.put(GeneratedKey(num1))
    assert(bf.contains(GeneratedKey(num1)))
  }
  it should "return false if element is not present" in {
    val f = new Fixture
    import f._
    val num1 = random.nextLong()
    bf.put(GeneratedKey(num1))
    assert(!bf.contains(GeneratedKey(num1+1)))
  }

  it should "BloomFilter put/contains" in {
    val f = new Fixture
    import f._
    val list = List.fill(100)(random.nextLong())
    val generateKeys = list.map(GeneratedKey)
    generateKeys.foreach(bf.put)
    val containsResult = generateKeys.map(bf.contains)
    val result = containsResult.fold(true)(_ && _)
    assert(result)
  }

}
