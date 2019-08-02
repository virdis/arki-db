
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
import scodec.bits._

/**
  * Represents KEYSIZE:KEY:VALUESIZE:VALUE:DELETED
  */

final case class KeyByteVector(underlying: ByteVector, size: Int)
final case class ValueByteVector(underlying: ByteVector, size: Int)

sealed abstract case class PayloadBuffer(underlying: ByteVector)

object PayloadBuffer {

  final def fromKeyValue(key: KeyByteVector, value: ValueByteVector): PayloadBuffer = {
    val keySizeBytesVector   = ByteVector.fromShort(s = key.size.toShort, ordering = ByteOrdering.BigEndian)
    val valueSizeBytesVector = ByteVector.fromShort(s = value.size.toShort, ordering = ByteOrdering.BigEndian)
    val isDeleted            = ByteVector.fromByte(Constants.FALSE_BYTES)
    new PayloadBuffer(
      keySizeBytesVector ++ key.underlying ++ valueSizeBytesVector ++ value.underlying ++ isDeleted
    ){}
  }

  final def fromBuffer(buff: ByteBuffer): PayloadBuffer = {
    buff.flip()
    new PayloadBuffer(ByteVector.view(buff)) {}
  }

  final def toKeyValueByteVector(underlying: ByteVector): (ByteVector, ByteVector) = {
    val byteBuffer        = underlying.toByteBuffer
    val keySizeInShort    =  byteBuffer.getShort
    val keyArray          = new Array[Byte](keySizeInShort)
    byteBuffer.get(keyArray)
    val valueSizeInShort  = byteBuffer.getShort
    val valueArrays       = new Array[Byte](valueSizeInShort)
    byteBuffer.get(valueArrays)
    (ByteVector.view(keyArray), ByteVector.view(valueArrays))
  }
}

