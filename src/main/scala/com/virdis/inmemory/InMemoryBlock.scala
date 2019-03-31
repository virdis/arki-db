
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

package com.virdis.inmemory

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger

import cats.effect._
import cats.effect.concurrent.Semaphore
import com.virdis.hashing.Hasher
import com.virdis.models.{FrozenInMemoryBlock, KeyByteVector, PayloadBuffer, ValueByteVector}
import com.virdis.utils.Config
import cats.implicits._
import com.virdis.io.BlockWriter
import scodec.bits.ByteVector

abstract class InMemoryBlock[F[_], Hash](
                                  val config: Config,
                                  val hasher: Hasher[Hash]
                                  )(implicit F: Sync[F], T: Concurrent[F], C: ContextShift[F]) {
  @volatile var cmap                   = new ConcurrentSkipListMap[Long, PayloadBuffer]()
  private final val currentPageOffSet  = new AtomicInteger(0)
  private final val pageCounter        = new AtomicInteger(0)
  private final val maxAllowedBytes    = new AtomicInteger(0)
  final val blockWriter                = new BlockWriter[F](config)

  @inline def getCurrentPageOffSet = currentPageOffSet.get()
  @inline def getCurrentPage       = pageCounter.get()

  def add0(
            key: Long,
            payloadBuffer: PayloadBuffer,
            guard: F[Semaphore[F]]
          ): F[FrozenInMemoryBlock] = {
    val payloadSize      = payloadBuffer.underlying.size.toInt
    val entrySizeInBytes = config.indexKeySize + payloadSize
    println(s"Config PageSize=${config.pageSize} BlockSize=${config.blockSize} AllowedBytes=${config.maxAllowedBlockSize}")
    println(s"Key=${key} EntrySizeInBytes=${entrySizeInBytes} PayloadBuffer=${payloadSize}")
    F.ifM(F.delay(maxAllowedBytes.addAndGet(entrySizeInBytes) < config.maxAllowedBlockSize))(
      F.ifM(F.delay(currentPageOffSet.addAndGet(payloadSize) <= config.pageSize))(
        F.suspend {
          println(s"PAGEOFFSET TRUE CMAP=${cmap.size()} MaxAllowedBytes=${maxAllowedBytes.get()} PageCounter=${pageCounter.get()} CurrentPageOff=${currentPageOffSet.get()}")
          F.delay(cmap.put(key, payloadBuffer)) *> F.delay(FrozenInMemoryBlock.EMPTY)
        },
        F.suspend {
          F.ifM(F.delay(maxAllowedBytes.get() + config.pageSize > config.maxAllowedBlockSize))(
            resetCounters(key, payloadBuffer, guard, entrySizeInBytes, payloadSize),
            F.suspend {
              println(s"MAX+PAGESIZE CMAP=${cmap.size()} MaxAllowedBytes=${maxAllowedBytes.get()} PageCounter=${pageCounter.get()} CurrentPageOff=${currentPageOffSet.get()}")
              currentPageOffSet.set(0)
              currentPageOffSet.addAndGet(payloadSize)
              pageCounter.incrementAndGet()
              F.delay(cmap.put(key, payloadBuffer)) *> F.delay(FrozenInMemoryBlock.EMPTY)
            }
          )
        }
      ),
      resetCounters(key, payloadBuffer, guard, entrySizeInBytes, payloadSize)
      )
  }

  /***
    * Resets the all the counters and returns [[FrozenInMemoryBlock]]
    * [[Semaphore]] is used here as a "latch" to synchronize.
    * @param key
    * @param payloadBuffer
    * @param guard
    * @param entrySizeInBytes
    * @return
    */
  private def resetCounters(
                             key: Long,
                             payloadBuffer: PayloadBuffer,
                             guard: F[Semaphore[F]],
                             entrySizeInBytes: Int,
                             payloadSize: Int
                           ): F[FrozenInMemoryBlock] = {
    F.suspend {
      F.flatMap(guard) {
        semaphore =>
          // latch for reassigning the block
          println(s"RESET COUNTERs CMAP=${cmap.size()} MaxAllowedBytes=${maxAllowedBytes.get()} PageCounter=${pageCounter.get()} CurrentPageOff=${currentPageOffSet.get()}")
          semaphore.withPermit {
            val block = FrozenInMemoryBlock(cmap, pageCounter.get() + 1)
            pageCounter.set(0)
            currentPageOffSet.set(0)
            maxAllowedBytes.set(0)
            val offset = currentPageOffSet.addAndGet(payloadSize)
            maxAllowedBytes.addAndGet(entrySizeInBytes)
            cmap = new ConcurrentSkipListMap[Long, PayloadBuffer]()
            F.delay(cmap.put(key, payloadBuffer)) *> F.delay(block)
          }
      }
    }
  }

  //TODO change this to add FIMB to a queue
  def add(key: ByteBuffer, value: ByteBuffer, guard: F[Semaphore[F]]): F[FrozenInMemoryBlock] = {
    for {
      genratedKey  <- F.delay {
        hasher.hash(key.array())
      }
      (k, v) = makeByteVectors(key, value)
      fiber        <- T.start(add0(genratedKey.underlying, PayloadBuffer.fromKeyValue(k, v), guard))
      fimb         <- fiber.join
    } yield fimb

  }

  private def makeByteVectors(k: ByteBuffer, v: ByteBuffer) = {
    (KeyByteVector(ByteVector.view(k), k.capacity()), ValueByteVector(ByteVector.view(v), v.capacity()))
  }
}

