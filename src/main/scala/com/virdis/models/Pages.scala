
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

class Pages(val size: Int, val pageSize: Int) {
  private var _currentPageNo = 0
  val pages = new Array[ByteBuffer](size)
  (0 until size).foldLeft(pages) {
    (b, i) =>
      b(i) = ByteBuffer.allocateDirect(pageSize)
      b
  }

  def calculatePageNo(size: Int): Int = {
    val currentSize = getPosition
    if (currentSize + size < pageSize) getPageNo else {
      incrementPageNo
      getPageNo
    }
  }

  @inline def incrementPageNo: Unit = _currentPageNo += + 1
  @inline def getPageNo: Int     = _currentPageNo
  @inline def getPosition: Int   = pages(getPageNo).position()

  def add(payload: ByteBuffer): (Page,Offset) = {
    val pgNumber = calculatePageNo(payload.capacity())
    val currentOffset = getPosition
    pages(pgNumber).put(payload)
    (Page(pgNumber), Offset(currentOffset))
  }

}
