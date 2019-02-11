
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

import com.virdis.utils.Config
import com.virdis.utils.Tags.MyConfig

final class MergeBlockResult(config: Config[MyConfig]) {
  var currentTotal = 0
  // lets over allocate data buffer and index
  val pages1: Pages           = new Pages(config.pagesFromAllowBlockSize, config.pageSize)
  val pages2: Pages           = new Pages(config.pagesFromAllowBlockSize, config.pageSize)
  val index1: IndexByteBuffer = new IndexByteBuffer(ByteBuffer.allocateDirect(config.maxAllowedBlockSize / 2))
  val index2: IndexByteBuffer = new IndexByteBuffer(ByteBuffer.allocateDirect(config.maxAllowedBlockSize / 2))

  def add(key: Key, payloadBuffer: PayloadBuffer) = {
    if (switchPages(payloadBuffer)) {
      val (page, offset) = pages1.add(payloadBuffer)
      index1.add(key, page, offset)
    } else {
      val (page, offset) = pages2.add(payloadBuffer)
      index2.add(key, page, offset)
    }
    currentTotal += payloadBuffer.payload.capacity() + config.indexKeySize
  }

  @inline def switchPages(payloadBuffer: PayloadBuffer): Boolean =
     currentTotal + payloadBuffer.payload.capacity() + config.indexKeySize < config.maxAllowedBlockSize

}
