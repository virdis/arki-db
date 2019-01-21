
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

package com.virdis.writer

import java.nio.ByteBuffer

import cats.effect.IO
import com.virdis.utils.Constants._
import org.scalacheck.Gen

trait CommonFixtures {
 implicit val cf = IO.contextShift(scala.concurrent.ExecutionContext.global)

 lazy val addDataToMap: java.util.NavigableMap[Long, ByteBuffer] = {
  var totalCount = 0
  val allowedSize = SIXTY_FOUR_MB_BYTES - (BLOOM_FILTER_SIZE + FOOTER_SIZE)
  val map = new java.util.TreeMap[Long, ByteBuffer]
  var data = 1
  while (totalCount < allowedSize) {
   val b = ByteBuffer.allocate(8)
   b.putInt(data) // add key
   b.putInt(data) // add value
   map.put(data, b)
   data += 1
   val payload = 2 * INT_SIZE_IN_BYTES // key + value size in bytes
   totalCount += LONG_SIZE_IN_BYTES + payload + INDEX_KEY_SIZE
  }
  println(s"ALLOWED SIZE=${allowedSize}")
  println(s"TOTAL BYTES ADDED TO MAP SIZE=${totalCount} BYTES")
  println(s"MAP DETAILS NUMBER OF KEYS=${map.size()}")
  map
 }

 def genSearchKey(upperBound: Long) = {
   Gen.choose(1, upperBound)
 }
 def genListOfSearchKeys(noOfKeys: Int, upBound: Long) = {
   Gen.listOfN[Long](noOfKeys, genSearchKey(upBound))
 }

}
