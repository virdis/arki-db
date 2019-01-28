
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

package com.virdis.io

import java.nio.ByteBuffer

import cats.effect.{ContextShift, Sync}
import com.virdis.models._
import com.virdis.utils.{Constants, Utils}
import Constants._

abstract class MergeBlock[F[_]]()(implicit F: Sync[F], Cs: ContextShift[F]){

  def payloadByteBuffers(
     idx: Index,
     dataBuffer: ByteBuffer
     ): PayloadBuffer = {
    // dont duplicate the buffer we need to move the data buffer pointers
    dataBuffer.position(idx.underlying)
    val keySize = dataBuffer.getShort
    val key     = new Array[Byte](keySize)
    dataBuffer.position(idx.underlying + 2)
    dataBuffer.get(key)
    dataBuffer.position(idx.underlying + 2 + keySize)
    val valueSize = dataBuffer.getShort
    val value     = new Array[Byte](valueSize)
    dataBuffer.position(idx.underlying + 2 + keySize + 2)
    dataBuffer.get(value)
    // Since byte buffer is backed by arrays no need to flip
    PayloadBuffer(ByteBuffer.wrap(key), ByteBuffer.wrap(value))
  }

  //INDEX : KEY:PAGENO:OFFSET
  @inline def payloadOffSet(index: ByteBuffer): IndexElement =
    IndexElement(
      key    = Key(index.getLong()),
      page   = Page(index.getInt()),
      offSet = Offset(index.getInt())
    )

  def compareKeys(p1: PayloadBuffer, p2: PayloadBuffer) = {
    val keyBuff1 = p1.key.duplicate()
    val keyBuff2 = p2.key.duplicate()
    val key1 = keyBuff1.getLong
    val key2 = keyBuff2.getLong
    Utils.freeDirectBuffer(keyBuff1)(F, Cs)
    Utils.freeDirectBuffer(keyBuff2)(F, Cs)
    key1.compareTo(key2)
  }

  def merge(b1: Block, b2: Block): List[Block] = {
    val indexBuffer = collection.mutable.ListBuffer[ByteBuffer]
    val dataBuffer  = collection.mutable.ListBuffer[ByteBuffer]

    def go(
          b1: Block,
          b2: Block,
          pageNumber: Int,
          noOfKeys: Int,
          dataCounter: Int,
          accumulator: ByteBuffer
          ): ByteBuffer = {

      if(b1.index.hasRemaining && b2.index.hasRemaining){
        val ielement1 = payloadOffSet(b1.index)
        val ielement2 = payloadOffSet(b2.index)
        val dataOffSet1 = Utils.calculateOffset(ielement1)
        val dataOffSet2 = Utils.calculateOffset(ielement2)
        val payload1 = payloadByteBuffers(Index(dataOffSet1), b1.data)
        val payload2 = payloadByteBuffers(Index(dataOffSet2), b2.data)
        val compare  = compareKeys(payload1, payload2)
        if(compare == -1) {
          //payload1 is smaller
        } else if (compare == 1) {
          //payload1 is bigger
        } else {

        }
      } else if (b1.index.hasRemaining) {

      } else if (b2.index.hasRemaining) {

      }


      ???
    }
    // call go method with duplicate ByteBuffers
    // make sure to flip them
    ???
  }

}
