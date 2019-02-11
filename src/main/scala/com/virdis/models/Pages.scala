
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

package com.virdis.models

import java.nio.ByteBuffer

final class Pages(val size: Int, val pageSize: Int) {
  private var _currentPageNo = 0

  val pages = new Array[ByteBuffer](size)
  (0 until size).foldLeft(pages) {
    (b, i) =>
      b(i) = ByteBuffer.allocateDirect(pageSize)
      b
  }

  def calculatePageNo(payloadSize: Int): Int = {
    val currentSize = getPosition
    if (currentSize + payloadSize < pageSize) getPageNo else {
      incrementPageNo
      getPageNo
    }
  }

  /**
    * This should never go over size since when building a block its
    * we will be less than the maxAllowedBlockSize
    */
  @inline def incrementPageNo: Unit = _currentPageNo += + 1
  @inline def getPageNo: Int        = _currentPageNo
  @inline def getPosition: Int      = pages(getPageNo).position()

  /**
    * This method returns the current Page where the
    * [[PayloadBuffer]] was added and the [[Offset]] in the page
    * BEFORE the [[PayloadBuffer]] was added.
    * @param pb [[PayloadBuffer]]
    * @return [[Page]] , [[Offset]]
    */
  def add(pb: PayloadBuffer): (Page,Offset) = {
    val pgNumber = calculatePageNo(pb.payload.capacity())
    val currentOffset = getPosition
    pb.payload.flip()
    pages(pgNumber).put(pb.payload)
    (Page(pgNumber), Offset(currentOffset))
  }

}
