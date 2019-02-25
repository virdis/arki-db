
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

/**
  * Represents KEYSIZE:KEY:VALUESIZE:VALUE:DELETED
  * @param payload
  */
sealed abstract case class PayloadBuffer(underlying: ByteBuffer)

object PayloadBuffer {

  def fromKeyValue(key: ByteBuffer, value: ByteBuffer): PayloadBuffer = {
    val dupKey   = key.duplicate()
    val dupValue = value.duplicate()
    val buffer   = ByteBuffer.allocate(5 + key.capacity() + value.capacity() )
    dupKey.flip()
    dupValue.flip()
    buffer.putShort(dupKey.capacity().toShort)
    buffer.put(dupKey)
    buffer.putShort(dupValue.capacity().toShort)
    buffer.put(dupValue)
    buffer.put(Constants.TRUE_BYTES)
    new PayloadBuffer(buffer){}
  }

  def fromBuffer(buff: ByteBuffer): PayloadBuffer = new PayloadBuffer(buff) {}
}

