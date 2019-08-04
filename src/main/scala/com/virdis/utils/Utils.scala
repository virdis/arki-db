
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

package com.virdis.utils

import java.nio.{Buffer, ByteBuffer}

import com.virdis.threadpools.IOThreadFactory._
import cats.effect.{ContextShift, Sync}
import Constants._
import com.virdis.models.{Footer, MaxKey, MinKey, Offset, Page}
import cats.implicits._

object Utils {

  // TODO document
  @inline final def freeDirectBuffer[F[_]](nativeAddress: Long)(F: Sync[F], C: ContextShift[F]): F[Unit] =
    C.evalOn(blockingIOPool.executionContext)(F.delay(memoryManager.freeMemory(nativeAddress)))

  final def freeDirectBuffer[F[_]](buffer: Buffer)(F: Sync[F], Cs: ContextShift[F]): F[Unit] =
    F.ifM(F.delay(buffer.isDirect))(
      Cs.evalOn(blockingIOPool.executionContext)(F.delay {
        val nativeAddress = memoryManager.getDirectBufferAddress(buffer)
        memoryManager.freeMemory(nativeAddress)
      }),
      F.unit
    )

  @inline final def calculateOffset(page: Page, offSet: Offset, pageSize: Int): Int = (page.underlying * pageSize) + offSet.underlying

  @inline final def kvByteBuffers(idx: Int, dataBuffer: ByteBuffer): (Array[Byte], Array[Byte]) = {
    dataBuffer.position(idx)
    val keySize = dataBuffer.getShort
    val key = new Array[Byte](keySize)
    dataBuffer.position(idx + 2)
    dataBuffer.get(key)
    dataBuffer.position(idx + 2 + keySize)
    val valueSize = dataBuffer.getShort
    val value = new Array[Byte](valueSize)
    dataBuffer.position(idx + 2 + keySize + 2)
    dataBuffer.get(value)
    (key, value)
  }

  //http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
  @inline def nextPowerOfTwo(i: Int): Int = {
    var x = i - 1
    x = x | (x >> 1)
    x = x | (x >> 2)
    x = x | (x >> 4)
    x = x | (x >> 8)
    x = x | (x >> 16)
    x + 1
  }

  @inline final def buildKey(footer: Footer): String =
    buildKey(footer.minKey, footer.maxKey)

  @inline final def buildKey(minKey: MinKey, maxKey: MaxKey): String =
    minKey.underlying.toString +":"+ maxKey.underlying.toString

  @inline final def duplicateAndFlipBuffer(buffer: ByteBuffer): ByteBuffer = {
    val duplicate = buffer.duplicate()
    duplicate.flip()
    duplicate
  }
}
