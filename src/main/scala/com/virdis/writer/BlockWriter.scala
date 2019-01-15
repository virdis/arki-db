
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

package com.virdis.writer

import java.nio.{Buffer, ByteBuffer}

import com.virdis.utils.Constants._
import cats.effect.Sync
import cats.effect.concurrent.Ref
import com.virdis.utils.Utils

abstract class BlockWriter[F[_]]()(implicit F: Sync[F]) {

  // TODO Document
  final class PageAligned() {
    final val pgBuffer = Ref.of[F, ByteBuffer](ByteBuffer.allocateDirect(PAGE_SIZE.toInt))

    def debug: F[Unit] =
      F.flatMap(pgBuffer) {
        refBuff => F.flatMap(refBuff.get) {
          buffer =>
            F.delay(println(s"PageAligned[pgBuffer=${buffer}]"))
        }
      }
    def position: F[Int] =
      F.flatMap(pgBuffer) {
        refBuff  => F.flatMap(refBuff.get) {
          buffer => F.point(buffer.position())
        }
      }
    def hasRemaining: F[Boolean] =
      F.flatMap(pgBuffer) {
        refBuff  => F.flatMap(refBuff.get) {
          buffer => F.point(buffer.hasRemaining)
        }
      }
    def remaining: F[Int] =
      F.flatMap(pgBuffer) {
        refBuff  => F.flatMap(refBuff.get) {
          buffer => F.point(buffer.remaining())
        }
      }
    // TODO : remember to flip input data before calling put
    def put(data: ByteBuffer): F[Unit] =
      F.flatMap(pgBuffer) {
        refBuff     => F.flatMap(F.point(data)){
          toBeAdded => refBuff.modify[Unit]{
            buffer  =>(buffer.put(toBeAdded), ())
          }
        }
      }

    def flip: F[Buffer] =
      F.flatMap(pgBuffer) {
        refBuff   => F.flatMap(refBuff.get){
          buffer  => F.point(buffer.flip())
        }
      }

    def nativeAddress: F[Long] =
      F.flatMap(pgBuffer) {
        refBuff => F.flatMap(refBuff.get) {
          buffer => F.delay(memoryManager.getDirectBufferAddress(buffer))
        }
      }

  }

  /*###################################################################################*/

  // TODO DRAW ASCII DIAGRAM OF BLOCK

  def build(map: java.util.NavigableMap[Long, ByteBuffer], dataSize: Int) = {
    val keySet = map.navigableKeySet()
    val indexBuffer = ByteBuffer.allocateDirect(keySet.size() * INDEX_KEY_SIZE)
    val dataBufferSize = SIXTY_FOUR_MB_BYTES - (BLOOM_FILTER_SIZE + FOOTER_SIZE + indexBuffer.capacity())
    val dataBuffer = ByteBuffer.allocateDirect(dataBufferSize)
    val numberOfPagesRequired = Math.ceil(dataBufferSize.toDouble / PAGE_SIZE).toInt
    val keyIterator = keySet.iterator()
    var currentPageSize = 0
    while(keyIterator.hasNext) {
      val key = keyIterator.next()
    }
    def go(generatedKeys: java.util.Iterator[Long], noOfPage: Int, accumulator: ByteBuffer): ByteBuffer = {
      if (generatedKeys.hasNext) {
        val key = generatedKeys.next()
        val payLoadBuffer = map.get(key)
        payLoadBuffer.flip()
        // check current bb size
        if (accumulator.position + payLoadBuffer.capacity() < PAGE_SIZE) {
          accumulator.put(payLoadBuffer)
          indexBuffer.putLong(key)
          indexBuffer.putLong(dataBuffer.position())
          go(generatedKeys, noOfPage, accumulator)
        } else {
          accumulator.flip()
          dataBuffer.put(accumulator) // how to clean up, maybe use the same bytebuffer
          Utils.freeDirectBuffer(accumulator)
          val accBuffer = ByteBuffer.allocateDirect(PAGE_SIZE.toInt) // how to cleanup
          accBuffer.put(payLoadBuffer)
          indexBuffer.putLong(key)
          indexBuffer.putLong(dataBuffer.position())
          go(generatedKeys, noOfPage, accBuffer)

        }
      } else {
        accumulator
      }
      
    }
  }



}
