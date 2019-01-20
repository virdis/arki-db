
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

package com.virdis.search

import java.nio.ByteBuffer

import cats.effect.Sync

abstract class BlockIndexSearch[F[_]](implicit F: Sync[F]) {

  private def binarySearchByteBuffer0(
                               searchBuffer: ByteBuffer,
                               searchKey: Long,
                               skipKeySize: Int,
                               start: Int = 0,
                               end:   Int
): Long = {
    println(s"Search Key = ${searchKey}")
    var lo = start
    var hi = end - 1
    var midValue = -1L
    while(lo <= hi) {
      //   println(s"Buffer Low=${lo} Buffer High=${hi}")
      val mid = (hi + lo) / 2
      // println(s"Buffer Mid=${mid}")
      val midValue = searchBuffer.getLong(mid * skipKeySize)
      // println(s"MidValue=${midValue}")
      if (midValue < searchKey) {
        // println("Search Key Bigger")
        lo = mid + 1
      } else if (midValue > searchKey) {
        //  println("Search Key Smaller")
        hi = mid - 1
      } else {
        return midValue
      }
    }
    midValue
  }

  def binarySearch(
                    searchBuffer: ByteBuffer,
                    searchKey: Long,
                    skipKeySize: Int,
                    start: Int = 0,
                    end:   Int
                  ): F[Long] =
    F.delay(binarySearchByteBuffer0(searchBuffer, searchKey, skipKeySize, start, end))

}
