
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
import java.util

import cats.effect.{ContextShift, Sync}
import com.virdis.models._
import com.virdis.utils.{Constants, Utils}
import Constants._

abstract class MergeBlock[F[_]]()(implicit F: Sync[F], Cs: ContextShift[F]){

  final def payloadByteBuffers(
     idx: Index,
     generatedKey: Long,
     dataBuffer: ByteBuffer
     ): PayloadBuffer = {
    // dont duplicate the buffer we need to move the data buffer pointers
    dataBuffer.position(idx.underlying)
    val keySize = dataBuffer.getShort
    val key     = new Array[Byte](keySize)
    // TODO :: Even though we have Index value class,
    //  arithmetic makes Boxing an issue, maybe specialize
    dataBuffer.position(idx.underlying + 2)
    dataBuffer.get(key)
    dataBuffer.position(idx.underlying + 2 + keySize)
    val valueSize = dataBuffer.getShort
    val value     = new Array[Byte](valueSize)
    dataBuffer.position(idx.underlying + 2 + keySize + 2)
    dataBuffer.get(value)
    // Since byte buffer is backed by arrays no need to flip
    val keyBuff      = ByteBuffer.wrap(key)
    val valueBuff    = ByteBuffer.wrap(value)
    val payload      = ByteBuffer.allocate(keyBuff.capacity() + valueBuff.capacity())
    payload.put(keyBuff)
    payload.put(valueBuff)
    payload.flip() // ready to use
    PayloadBuffer(generatedKey, payload)
  }

  //INDEX : KEY:PAGENO:OFFSET
  @inline final def payloadOffSet(index: ByteBuffer): IndexElement =
    IndexElement(
      key    = Key(index.getLong()),
      page   = Page(index.getInt()),
      offSet = Offset(index.getInt())
    )

  @inline final def compareKeys(p1: PayloadBuffer, p2: PayloadBuffer): Int = p1.key.compareTo(p2.key)

  @inline final def compareTs(ts1: Long, ts2: Long, p1: PayloadBuffer, p2: PayloadBuffer): PayloadBuffer = {
    if (ts1 > ts2) p1 else p2
  }

  @inline def calculateFlag(payloadBuffer: PayloadBuffer, total: Int): Boolean =
    SIXTY_FOUR_MB_BYTES - (BLOOM_FILTER_SIZE + FOOTER_SIZE) > total

  final def merge(
             b1: Block,
             b2: Block,
           ): MergeBlockResult = {

      var currentTotal = 0
      val mergeBlockResult = new MergeBlockResult()

      while (b1.index.hasRemaining &&  b2.index.hasRemaining) {
        val ielement1: IndexElement = payloadOffSet(b1.index)
        val ielement2: IndexElement = payloadOffSet(b2.index)
        val dataOffSet1 = Utils.calculateOffset(ielement1)
        val dataOffSet2 = Utils.calculateOffset(ielement2)
        val payload1: PayloadBuffer = payloadByteBuffers(Index(dataOffSet1), ielement1.key.underlying, b1.data)
        val payload2: PayloadBuffer = payloadByteBuffers(Index(dataOffSet2), ielement2.key.underlying, b2.data)

        if (payload1.key != payload2.key) {
          currentTotal += payload1.sizeInBytes
          mergeBlockResult.add(payload1, calculateFlag(payload1, currentTotal))
          currentTotal += payload2.sizeInBytes
          mergeBlockResult.add(payload2, calculateFlag(payload2, currentTotal))
        } else {
          val payload = compareTs(
            b1.footer.timeStamp.underlying,
            b2.footer.timeStamp.underlying,
            payload1, payload2)
          currentTotal += payload.sizeInBytes
          mergeBlockResult.add(payload, calculateFlag(payload, currentTotal))
        }
      }
      while (b1.index.hasRemaining) {
        val ielement1   = payloadOffSet(b1.index)
        val dataOffSet1 = Utils.calculateOffset(ielement1)
        val payload1    = payloadByteBuffers(Index(dataOffSet1), ielement1.key.underlying, b1.data)
        currentTotal += payload1.sizeInBytes
        mergeBlockResult.add(payload1, calculateFlag(payload1, currentTotal))
      }
      while (b2.index.hasRemaining) {
        val ielement2   = payloadOffSet( b2.index)
        val dataOffSet2 = Utils.calculateOffset(ielement2)
        val payload2    = payloadByteBuffers(Index(dataOffSet2), ielement2.key.underlying, b2.data)
        currentTotal += payload2.sizeInBytes
        mergeBlockResult.add(payload2, calculateFlag(payload2, currentTotal))
      }

    mergeBlockResult
  }


}
