
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
import cats.effect.Sync
import cats.effect.concurrent.Semaphore
import com.virdis.hashing.Hasher
import com.virdis.models.{FrozenInMemoryBlock, PayloadBuffer}
import com.virdis.utils.Config

abstract class InMemoryBlock[F[_], Hash](
                                  config: Config,
                                  hasher: Hasher[Hash]
                                  )(implicit F: Sync[F]) {
  @volatile var cmap                 = new ConcurrentSkipListMap[Long, PayloadBuffer]()
  private final val currentPageSize  = new AtomicInteger(0)
  private final val totalPages       = new AtomicInteger(0)
  final val allowedPages = config.pagesFromAllowBlockSize - 1

  def add0(
            key: Long,
            payloadBuffer: PayloadBuffer,
            guard: F[Semaphore[F]]
          ): F[FrozenInMemoryBlock] = {
    var block = FrozenInMemoryBlock.EMPTY
    F.ifM(F.delay(currentPageSize.addAndGet(config.indexKeySize + payloadBuffer.underlying.capacity()) > config.pageSize))({
      F.ifM(F.delay(totalPages.incrementAndGet() == allowedPages))({
        F.flatMap(guard) {
          semaphore =>
            // latch for resassign the block
            semaphore.withPermit {
              block = FrozenInMemoryBlock(cmap)
              totalPages.set(0)
              cmap = new ConcurrentSkipListMap[Long, PayloadBuffer]()
              F.delay(block)
            }
        }
      },
        F.suspend {
          currentPageSize.set(0)
          currentPageSize.addAndGet(config.indexKeySize + payloadBuffer.underlying.capacity())
          cmap.put(key, payloadBuffer)
          F.delay(block)
        })
    },
      F.suspend {
        cmap.put(key, payloadBuffer)
        F.delay(block)
      })
  }

  def add(key: ByteBuffer, value: ByteBuffer, guard: F[Semaphore[F]]) = {
    F.flatMap(F.delay {
      val duplicateKey = key.duplicate()
      hasher.hash(duplicateKey)
    }) {
      generatedKey =>
        add0(generatedKey.underlying, PayloadBuffer.fromKeyValue(key, value), guard)
      }
  }
}





/*if (currentPageSize.addAndGet(config.indexKeySize + payloadBuffer.underlying.capacity()) > config.pageSize) {
  if (totalPages.incrementAndGet() == allowedPages) { // incremented pages
    F.flatMap(guard){
      semaphore =>
        semaphore.withPermit {
          block = FrozenInMemoryBlock(cmap)
          totalPages.set(0)
          cmap = new ConcurrentSkipListMap[Long, PayloadBuffer]()
          F.unit // change this to make it pure
        }
    }
  }
  currentPageSize.set(0)
  currentPageSize.addAndGet(config.indexKeySize + payloadBuffer.underlying.capacity())
  cmap.put(key, payloadBuffer)
} else {
  cmap.put(key, payloadBuffer)
}
block*/










