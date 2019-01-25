
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

import cats.effect.{ContextShift, IO}
import com.virdis.utils.Constants._
import org.scalacheck.Gen

trait CommonFixtures {
  implicit val cf: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)

  lazy val addDataToMap: java.util.NavigableMap[Long, ByteBuffer] = CommonFixtures.generatedMap
  lazy val smallData = CommonFixtures.smallDataMap

 def genSearchKey(upperBound: Long) = {
   Gen.choose(1, upperBound)
 }
 def genListOfSearchKeys(noOfKeys: Int, upBound: Long) = {
   Gen.listOfN[Long](noOfKeys, genSearchKey(upBound))
 }

}

object CommonFixtures {
  // Let's build this once
  val generatedMap = {
    var totalCount = 0
    val allowedSize = SIXTY_FOUR_MB_BYTES - (BLOOM_FILTER_SIZE + FOOTER_SIZE)
    val map = new java.util.TreeMap[Long, ByteBuffer]
    var data = 1
    while (totalCount < allowedSize) {
      val b = ByteBuffer.allocate(2 + 4 + 2 + 4 + 1) //KEYSIZE:KEY:VALUESIZE:VALUE:ISDELETED
      b.putShort(4) // add key size
      b.putInt(data) // add key
      b.putShort(4) // add value size
      b.putInt(data) // add value
      b.put(0.toByte) // isDeleted
      map.put(data, b)
      data += 1
      // KEYSIZE(SMALLINT):KEY:VALUESIZE(SMALLINT):VALUE:ISDELETED
      val payload = (2 * INT_SIZE_IN_BYTES) + (2 * SHORT_SIZE_IN_BYTES) + BYTE_SIZE_IN_BYTES
      totalCount += LONG_SIZE_IN_BYTES + payload + INDEX_KEY_SIZE // KEY:VALUE:INDEXKEYSIZE
    }
    println(s"ALLOWED SIZE=${allowedSize}")
    println(s"TOTAL BYTES ADDED TO MAP SIZE=${totalCount} BYTES")
    println(s"MAP DETAILS NUMBER OF KEYS=${map.size()}")
    map
  }

  val smallDataMap = {
    val map = new java.util.TreeMap[Long, ByteBuffer]
    (1 to 100) .foreach {
      i =>
        val b = ByteBuffer.allocate(2 + 4 + 2 + 4 + 1)
        b.putShort(4) // add key size
        b.putInt(i) // add key
        b.putShort(4) // add value size
        b.putInt(i) // add value
        b.put(0.toByte) // isDeleted
        map.put(i, b)
    }
    map
  }
}
