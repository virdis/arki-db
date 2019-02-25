
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

final case class DataByteBuffer(val underlying: ByteBuffer) extends AnyVal {
  def getDataBufferElement(idx: Int) = {
    underlying.position(idx)
    val keySize = underlying.getShort
    val key = new Array[Byte](keySize)
    underlying.get(key)
    val valueSize = underlying.getShort
    val value = new Array[Byte](valueSize)
    underlying.get(value)
    val isDelete = underlying.get()
    val byteBuffer = ByteBuffer.allocate(5 + keySize + valueSize)
    byteBuffer.putShort(keySize)
    byteBuffer.put(key)
    byteBuffer.putShort(valueSize)
    byteBuffer.put(value)
    byteBuffer.put(isDelete)
    byteBuffer.flip()
    PayloadBuffer.fromBuffer(byteBuffer)
  }
}
