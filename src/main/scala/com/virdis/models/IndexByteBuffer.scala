
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

import com.virdis.utils.Constants


final case class IndexByteBuffer(val byteBuffer: ByteBuffer) extends AnyVal {
  // We dont create a duplicate since we want the internal Buffer counters to increment
  def getIndexElement: (Key, Page, Offset) = {
    val key    = byteBuffer.getLong
    val page   = byteBuffer.getInt
    val offSet = byteBuffer.getInt
    (Key(key), Page(page), Offset(offSet))
  }

  @inline def checkBounds: Boolean = byteBuffer.position() + Constants.INDEX_KEY_SIZE < byteBuffer.capacity()

  def add(key: Key, page: Page, offset: Offset) = {
    byteBuffer.putLong(key.underlying)
    byteBuffer.putInt(page.underlying)
    byteBuffer.putInt(offset.underlying)
  }

}

