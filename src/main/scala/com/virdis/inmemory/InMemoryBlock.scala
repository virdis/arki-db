
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

import cats.FlatMap
import cats.effect._
import cats.effect.concurrent.Semaphore
import com.virdis.hashing.Hasher
import com.virdis.models.{BlockWriterResult, FrozenInMemoryBlock, PayloadBuffer}
import com.virdis.utils.Config
import cats.implicits._
import com.virdis.io.BlockWriter

abstract class InMemoryBlock[F[_], Hash](
                                  val config: Config,
                                  val hasher: Hasher[Hash]
                                  )(implicit F: Sync[F], T: Concurrent[F], C: ContextShift[F]) {
  @volatile var cmap                   = new ConcurrentSkipListMap[Long, PayloadBuffer]()
  private final val currentPageOffSet  = new AtomicInteger(0)
  private final val pageCounter        = new AtomicInteger(0)
  private final val maxAllowedBytes    = new AtomicInteger(0)
  final val blockWriter = new BlockWriter[F](config)

  @inline def getCurrentPageOffSet = currentPageOffSet.get()
  @inline def getCurrentPage       = pageCounter.get()

  def add0(
            key: Long,
            payloadBuffer: PayloadBuffer,
            guard: F[Semaphore[F]]
          ): F[FrozenInMemoryBlock] = {
    println(s"Key=${key}")
    val entrySizeInBytes = config.indexKeySize + payloadBuffer.underlying.capacity()
    println(s"MaxedAllowedBytes=${maxAllowedBytes.get()}")
    F.ifM(F.delay(maxAllowedBytes.addAndGet(entrySizeInBytes) < config.maxAllowedBlockSize))(
      F.ifM(F.delay(currentPageOffSet.addAndGet(entrySizeInBytes) <= config.pageSize))(
        F.suspend {
          println(s"Inner True key=${key} CurrentPageSize=${currentPageOffSet}")
          F.delay(cmap.put(key, payloadBuffer)) *> F.delay(FrozenInMemoryBlock.EMPTY)
        },
        F.suspend {
          currentPageOffSet.set(0)
          val offset = currentPageOffSet.addAndGet(entrySizeInBytes)
          val pgCount = pageCounter.incrementAndGet()
          println(s"Inner False pagecounter=${pgCount} offSet=${offset}")
          F.delay(cmap.put(key, payloadBuffer)) *> F.delay(FrozenInMemoryBlock.EMPTY)
        }
      ),
      F.suspend {
        F.flatMap(guard) {
          semaphore =>
            // latch for reassigning the block
            semaphore.withPermit {
              val block = FrozenInMemoryBlock(cmap, pageCounter.get())
              pageCounter.set(0)
              currentPageOffSet.set(0)
              maxAllowedBytes.set(0)
              val offset = currentPageOffSet.addAndGet(entrySizeInBytes)
              maxAllowedBytes.addAndGet(entrySizeInBytes)
              println(s"Outer False CMap Size=${cmap.size()} offset=${offset}")
              cmap = new ConcurrentSkipListMap[Long, PayloadBuffer]()
              F.delay(cmap.put(key, payloadBuffer)) *> F.delay(block)
            }
        }
      })
  }

  //TODO change this to add FIMB to a queue
  def add(key: ByteBuffer, value: ByteBuffer, guard: F[Semaphore[F]])= {
    for {
      genratedKey  <- F.delay(hasher.hash(key.duplicate()))
      fiber        <- T.start(add0(genratedKey.underlying, PayloadBuffer.fromKeyValue(key, value), guard))
      fimb         <- fiber.join
    } yield fimb

  }

}

