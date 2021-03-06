
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
import com.virdis.models.{GeneratedKey, Offset, Page, SearchResult}

final class BlockIndexSearchF[F[_]](implicit F: Sync[F]) extends IndexSearch[F] {
  // TODO REMOVE PRINTLN
  private final def binarySearchByteBuffer0(
                               searchBuffer: ByteBuffer,
                               searchKey: Long,
                               skipKeySize: Int,
                               start: Int = 0,
                               end:   Int
): SearchResult = {
    searchBuffer.position(0)
    var lo = start
    var hi = end - 1
    var midValue = -1L
    while(lo <= hi) {
      val mid = (hi + lo) / 2
      val skip     = mid * skipKeySize
      val midValue = searchBuffer.getLong(skip)
      val page     = searchBuffer.getInt(skip + 8)
      val offSet   = searchBuffer.getInt(skip + 8 + 4)
      if (midValue < searchKey) {
        lo = mid + 1
      } else if (midValue > searchKey) {
        hi = mid - 1
      } else {
        return SearchResult(
          key     = GeneratedKey(searchKey),
          page    = Page(page),
          offSet  = Offset(offSet)
        )
      }
    }
    SearchResult.NOT_FOUND
  }

  final def binarySearch(
                    searchBuffer: ByteBuffer,
                    searchKey: Long,
                    skipKeySize: Int,
                    start: Int = 0,
                    end:   Int
                  ): F[SearchResult] =
    F.delay(binarySearchByteBuffer0(searchBuffer, searchKey, skipKeySize, start, end))
}
