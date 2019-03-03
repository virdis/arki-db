
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
import cats.effect.{Concurrent, ConcurrentEffect, Sync}
import cats.effect.concurrent.Semaphore
import com.virdis.hashing.Hasher
import com.virdis.models.{FrozenInMemoryBlock, PayloadBuffer}
import com.virdis.utils.Config
import cats.implicits._
import com.virdis.io.BlockWriter

abstract class InMemoryBlock[F[_], Hash](
                                  config: Config,
                                  hasher: Hasher[Hash],
                                  blockWriter: BlockWriter[F]
                                  )(implicit F: Sync[F], T: Concurrent[F]) {
  @volatile var cmap                   = new ConcurrentSkipListMap[Long, PayloadBuffer]()
  private final val currentPageOffSet  = new AtomicInteger(0)
  private final val currentPage        = new AtomicInteger(0)
  final val allowedPages               = config.pagesFromAllowBlockSize - 1

  @inline def getCurrentPageOffSet = currentPageOffSet.get()
  @inline def getCurrentPage       = currentPage.get()

  def add0(
            key: Long,
            payloadBuffer: PayloadBuffer,
            guard: F[Semaphore[F]]
          ): F[FrozenInMemoryBlock] = {
    println(s"Key=${key}")
    F.ifM(F.delay(currentPageOffSet.addAndGet(config.indexKeySize + payloadBuffer.underlying.capacity()) > config.pageSize))({
      println(s"Outer If Block=${key} CurrentPageSize=${currentPageOffSet}")
      F.ifM(F.delay(currentPage.incrementAndGet() == allowedPages))({ // increment pageSize
        F.flatMap(guard) {
          semaphore =>
            // latch for reassigning the block
            semaphore.withPermit {
              println(s"Config=${config.pagesFromAllowBlockSize}")
              println(s"MAP=${cmap.size()} CurrentPageSize=${currentPageOffSet.get()} totalPages=${currentPage.get()}")
              val block = FrozenInMemoryBlock(cmap)
              println(s"With Permit, map=${cmap.size()}")
              currentPage.set(0)
              cmap = new ConcurrentSkipListMap[Long, PayloadBuffer]()
              F.delay(block)
            }
        }
      },
        F.suspend {
          println(s"Inner Else Block=${key} CurrentPageSize=${currentPageOffSet}")
          currentPageOffSet.set(0)
          currentPageOffSet.addAndGet(config.indexKeySize + payloadBuffer.underlying.capacity())
          F.delay(cmap.put(key, payloadBuffer)) *> F.delay(FrozenInMemoryBlock.EMPTY)
        })
    },
      F.suspend {
        println(s"Outer Else Block=${key} CurrentPageSize=${currentPageOffSet}")
        F.delay(cmap.put(key, payloadBuffer)) *> F.delay(FrozenInMemoryBlock.EMPTY)
      })
  }

  def add(key: ByteBuffer, value: ByteBuffer, guard: F[Semaphore[F]]) = {
    val frozenImb: F[FrozenInMemoryBlock] = F.flatMap(F.delay {
      val duplicateKey = key.duplicate()
      hasher.hash(duplicateKey)
    }) {
      generatedKey =>
        add0(generatedKey.underlying, PayloadBuffer.fromKeyValue(key, value), guard)
      }
    T.flatMap(T.start(frozenImb)) {
      fiber =>
        T.flatMap(fiber.join) {
          fimb =>
            T.whenA(!fimb.isEmpty) {
              blockWriter.build(fimb.map)
            }
        }
    }
  }

}

