
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
import cats.effect.{ContextShift, Sync}
import cats.effect.concurrent.Ref
import com.virdis.models.BlockWriterResult
import com.virdis.threadpools.IOThreadFactory
import com.virdis.threadpools.ThreadPool.BlockingIOPool
import com.virdis.utils.Utils

abstract class BlockWriter[F[_]]()(implicit F: Sync[F], Cs: ContextShift[F]) {

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


  def build(map: java.util.NavigableMap[Long, ByteBuffer]) = {
    val keySet = map.navigableKeySet()
    val indexBuffer = ByteBuffer.allocateDirect(keySet.size() * INDEX_KEY_SIZE)
    println(s"INDEX BUFFER=${indexBuffer}")
    val dataBufferSize = SIXTY_FOUR_MB_BYTES - (BLOOM_FILTER_SIZE + FOOTER_SIZE + indexBuffer.capacity())
    println(s"DATA BUFFER SIZE=${dataBufferSize}")
    val dataBuffer = ByteBuffer.allocateDirect(dataBufferSize)
    val calculatedPages = Math.ceil(dataBufferSize.toDouble / PAGE_SIZE).toInt
    val keyIterator = keySet.iterator()
    def go(
            generatedKeys: java.util.Iterator[Long],
            pageNumber: Int,
            accumulator: ByteBuffer
          ): BlockWriterResult[Int] = {

      if (generatedKeys.hasNext) {
        val key = generatedKeys.next()
        val payLoadBuffer = map.get(key)
        payLoadBuffer.flip()
        // check current bb size
        if (accumulator.position + payLoadBuffer.capacity() < PAGE_SIZE) {
          val payLoadOffSetInPage = accumulator.position()
          accumulator.put(payLoadBuffer)
          // setting up index GENERATEDKEY:PAGENO:OFFSET
          indexBuffer.putLong(key)
          indexBuffer.putInt(pageNumber)
          indexBuffer.putInt(payLoadOffSetInPage)
          println(s"INSIDE IF KEY=${key} INDEX BUFFER=${indexBuffer.position()} DATABUFFER=${dataBuffer.position()}" +
            s"CALCULATED ADDRESS=${Utils.calculatePageAddress(pageNumber, payLoadOffSetInPage)}")

          go(generatedKeys, pageNumber, accumulator)
        } else {
          accumulator.flip()
          dataBuffer.put(accumulator)
          // Clean up
          Utils.freeDirectBuffer(accumulator)(F, Cs)

          val newAccumulator = ByteBuffer.allocateDirect(PAGE_SIZE.toInt)
          val newPageNumber  = pageNumber + 1
          val newPayLoadOffSetInPage = newAccumulator.position()
          newAccumulator.put(payLoadBuffer)
          // setting up index GENERATEDKEY:PAGENO:OFFSET
          indexBuffer.putLong(key)
          indexBuffer.putInt(newPageNumber)
          indexBuffer.putInt(newPayLoadOffSetInPage)
          // move DataBuffer Index to new calculated address
          dataBuffer.position(Utils.calculatePageAddress(newPageNumber, newPayLoadOffSetInPage))

          println(s"OUTSIDE IF KEY=${key} INDEX BUFFER=${indexBuffer.position()} DATABUFFER=${dataBuffer.position()}" +
            s"CALCULATED ADDRESS=${Utils.calculatePageAddress(newPageNumber, newPayLoadOffSetInPage)}")

          go(generatedKeys, pageNumber + 1, newAccumulator)

        }
      } else {
        println(s"ACCUMULATOR DETAILS==${accumulator}")
        println(s"DATA BUFFER DETAILS==${dataBuffer}")
        accumulator.flip()
        dataBuffer.put(accumulator)
        println(s"DATA BUFFER DETAILS AFTER ADDING ACC==${dataBuffer}")
        // no more elements in the iterator, lets free last PAGE Direct Buffer
        Utils.freeDirectBuffer(accumulator)(F, Cs)

        dataBuffer.flip()
        indexBuffer.flip()
        BlockWriterResult(
          dataByteBuffer = dataBuffer,
          indexByteBuffer =  indexBuffer,
          calculatedPages = calculatedPages,
          actualPages = pageNumber
        )
      }
    }
    F.flatMap(F.point(0)) {
      i =>
        Cs.evalOn(IOThreadFactory.BLOCKING_IO_POOL.executionContext){
           F.delay {
            go(keyIterator, i, ByteBuffer.allocateDirect(PAGE_SIZE.toInt))
          }
        }
    }

  }



}
