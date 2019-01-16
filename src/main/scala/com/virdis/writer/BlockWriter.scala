
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

    def go(generatedKeys: java.util.Iterator[Long], pageNumber: Int, accumulator: ByteBuffer): (ByteBuffer, ByteBuffer) = {
      if (generatedKeys.hasNext) {
        val key = generatedKeys.next()
        val payLoadBuffer = map.get(key)
        payLoadBuffer.flip()
        // check current bb size
        if (accumulator.position + payLoadBuffer.capacity() < PAGE_SIZE) {
          accumulator.put(payLoadBuffer)
          // setting up index GENERATEDKEY:PAGENO:OFFSET
          indexBuffer.putLong(key)
          indexBuffer.putInt(pageNumber)
          indexBuffer.putInt(accumulator.position())

          go(generatedKeys, pageNumber, accumulator)
        } else {
          accumulator.flip()
          dataBuffer.put(accumulator) // how to clean up, maybe use the same bytebuffer
          Utils.freeDirectBuffer(accumulator)(F)
          val newAccumulator = ByteBuffer.allocateDirect(PAGE_SIZE.toInt)
          val newPageNumber = pageNumber + 1
          newAccumulator.put(payLoadBuffer)
          // setting up index GENERATEDKEY:PAGENO:OFFSET
          indexBuffer.putLong(key)
          indexBuffer.putInt(newPageNumber)
          indexBuffer.putInt(newAccumulator.position())

          go(generatedKeys, pageNumber + 1, newAccumulator)

        }
      } else {
        // flip
        dataBuffer.flip()
        indexBuffer.flip()
        (dataBuffer, indexBuffer) // emit stats
      }
    }
    F.flatMap(F.point(0)) {
      i =>
        F.delay {
            go(keyIterator, i, ByteBuffer.allocateDirect(PAGE_SIZE.toInt))
        }
    }
  }




}
