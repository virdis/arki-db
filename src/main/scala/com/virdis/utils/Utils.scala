
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
import cats.{Applicative, Traverse}
import com.virdis.models.{Footer, IndexElement, Offset, Page, PageAlignedDataBuffer}
import cats.syntax.traverse
import cats.implicits._
import com.virdis.threadpools.IOThreadFactory

object Utils {

  // TODO document
  @inline final def freeDirectBuffer[F[_]](nativeAddress: Long)(F: Sync[F], Cs: ContextShift[F]): F[Unit] =
    Cs.evalOn(blockingIOPool.executionContext)(F.delay(memoryManager.freeMemory(nativeAddress)))

  final def freeDirectBuffer[F[_]](buffer: ByteBuffer)(F: Sync[F], Cs: ContextShift[F]): F[Unit] =
    Cs.evalOn(blockingIOPool.executionContext)(F.delay {
      val nativeAddress = memoryManager.getDirectBufferAddress(buffer)
      memoryManager.freeMemory(nativeAddress)
    })

  @inline final def calculateOffset(page: Page, offSet: Offset, pageSize: Int): Int = (page.underlying * pageSize) + offSet.underlying

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
    footer.minKey.underlying.toString + footer.maxKey.underlying.toString

}
