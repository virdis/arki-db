
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

import java.nio.ByteBuffer

import com.virdis.threadpools.IOThreadFactory._
import cats.effect.{ContextShift, Sync}
import Constants._
import com.virdis.models.IndexElement

object Utils {

  // TODO document
  @inline final def freeDirectBuffer[F[_]](nativeAddress: Long)(F: Sync[F], Cs: ContextShift[F]): F[Unit] =
    Cs.evalOn(BLOCKING_IO_POOL.executionContext)(F.delay(memoryManager.freeMemory(nativeAddress)))

  final def freeDirectBuffer[F[_]](buffer: ByteBuffer)(F: Sync[F], Cs: ContextShift[F]): F[Unit] =
    Cs.evalOn(BLOCKING_IO_POOL.executionContext)(F.delay {
      val nativeAddress = memoryManager.getDirectBufferAddress(buffer)
      memoryManager.freeMemory(nativeAddress)
    })

  @inline final def calculateOffset0(pageNo:Int, pageOffSet: Int): Int = (pageNo * Constants.PAGE_SIZE.toInt) + pageOffSet

  @inline final def calculateOffset(indexElement: IndexElement): Int = calculateOffset0(indexElement.page.underlying, indexElement.offSet.underlying)

  // TODO clean up Duplicate buffer
  final def kvByteBuffers[F[_]](idx: Int, dataBuffer: ByteBuffer)(F: Sync[F], Cs: ContextShift[F]) = {
    val duplicate = dataBuffer.duplicate()
    duplicate.position(idx)
    val keySize = duplicate.getShort
    val key = new Array[Byte](keySize)
    duplicate.position(idx + 2)
    duplicate.get(key)
    duplicate.position(idx + 2 + keySize)
    val valueSize = duplicate.getShort
    val value = new Array[Byte](valueSize)
    duplicate.position(idx + 2 + keySize + 2)
    duplicate.get(value)
    freeDirectBuffer(duplicate)(F, Cs)
    (key, value)
  }
}
