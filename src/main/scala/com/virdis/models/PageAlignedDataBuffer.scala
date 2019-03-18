
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
// should validate inputs???

final class PageAlignedDataBuffer(
                   val noOfPages: Int,
                   val pageSize: Int,
                   val buffer: ByteBuffer
                 ) {
  private var currentOffSet = 0
  private var page = 0

  def calculatePageAndOffSet(payloadSize: Int): (Page, Offset) = {
    if (currentOffSet + payloadSize <= pageSize) {
      val _currentOffSet = currentOffSet
      currentOffSet += payloadSize
      (Page(page), Offset(_currentOffSet))
    } else {
      page += 1
      val offSet = page * pageSize
      buffer.position(offSet)
      currentOffSet = 0
      currentOffSet += payloadSize
      (Page(page), Offset(0))
    }
  }

  /**
    * This method returns the current Page where the
    * [[PayloadBuffer]] was added and the [[Offset]] in the page
    * BEFORE the [[PayloadBuffer]] was added.
    * @param pb [[PayloadBuffer]]
    * @return [[Page]] , [[Offset]]
    */
  def add(pb: PayloadBuffer): (Page,Offset) = {
    val payloadBuff = pb.underlying.toByteBuffer
    val (page, offset) = calculatePageAndOffSet(payloadBuff.capacity())
    println(s"PAGE=${page} OFFSET=${offset} PAYLOADBUFFER=${payloadBuff}")
    buffer.put(payloadBuff)
    (page, offset)
  }

}
