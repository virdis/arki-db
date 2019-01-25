
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
import java.util.concurrent.{Executors, ThreadFactory}

import cats.effect.Sync
import Constants._

object Utils {

  // TODO document
  @inline final def freeDirectBuffer[F[_]](nativeAddress: Long)(F: Sync[F]): F[Unit] =
    F.delay(memoryManager.freeMemory(nativeAddress))

  final def freeDirectBuffer[F[_]](buffer: ByteBuffer)(F: Sync[F]): F[Unit] =
    F.delay {
      val nativeAddress = memoryManager.getDirectBufferAddress(buffer)
      memoryManager.freeMemory(nativeAddress)
    }

  final def calculatePageAddress(pageNo:Int, pageOffSet: Int): Int = {
    println( s"PAGE=${pageNo} OFFSET=${pageOffSet}" )
    (pageNo * Constants.PAGE_SIZE.toInt) + pageOffSet
  }

  // TODO clean up Duplicate buffer
  final def kvByteBuffers(idx: Int, dataBuffer: ByteBuffer) = {
    val duplicate = dataBuffer.duplicate()
    duplicate.position(idx)
    val keySize = duplicate.getShort
    println(s"KEYSIZE=${keySize}")
    val key = new Array[Byte](keySize)
    duplicate.position(idx + 2)
    duplicate.get(key)
    println(s"KEY=${key}")
    duplicate.position(idx + 2 + keySize)
    val valueSize = duplicate.getShort
    println(s"VALUESIZE=${valueSize}")
    val value = new Array[Byte](valueSize)
    duplicate.position(idx + 2 + keySize + 2)
    duplicate.get(value)
    println(s"VALUE=${value}")
    (key, value)
  }
}
