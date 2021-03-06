
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


final class IndexByteBuffer(val underlying: ByteBuffer) {
  private var _counter = 0
  // We dont create a duplicate since we want the internal Buffer counters to be incremented
  @inline final def getIndexElement: (GeneratedKey, Page, Offset) = {
    val key    = underlying.getLong
    val page   = underlying.getInt
    val offSet = underlying.getInt
    (GeneratedKey(key), Page(page), Offset(offSet))
  }

  @inline final def getCounter = _counter
  @inline final def incrementCounter = _counter = getCounter + 1


  @inline final def add(key: GeneratedKey, page: Page, offset: Offset) = {
    underlying.putLong(key.underlying)
    underlying.putInt(page.underlying)
    underlying.putInt(offset.underlying)
    incrementCounter
  }

}

