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

package com.virdis.hashing

import com.virdis.models.GeneratedKey
import com.virdis.utils.Constants
import net.jpountz.xxhash.{XXHash64, XXHashFactory}

trait Hasher[A] {
  def instance: A
  def hash(key: Array[Byte]): GeneratedKey
}

object Hasher {

  implicit val xxhash64: Hasher[XXHash64] = new Hasher[XXHash64] {
    @inline final override val instance: XXHash64 = XXHashFactory.fastestInstance().hash64()
    @inline final override def hash(key: Array[Byte]): GeneratedKey = GeneratedKey(instance.hash(key, 0, key.size, Constants.XXHASH_SEED))
  }
}