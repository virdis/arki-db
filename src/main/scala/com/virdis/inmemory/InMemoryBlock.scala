
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
import com.virdis.models.{ArKiResult, FrozenInMemoryBlock, KeyByteVector, PayloadBuffer, ValueByteVector}
import com.virdis.utils.Config
import cats.implicits._
import com.virdis.api.ArKiApi
import com.virdis.io.BlockWriter
import com.virdis.search.Search
import com.virdis.search.inmemory.InMemoryMapSearch
import com.virdis.threadpools.IOThreadFactory
import scodec.bits.ByteVector

final class InMemoryBlock[F[_], Hash](
                                          val config:    Config,
                                          val search:    Search[F],
                                          val hasher:    Hasher[Hash],
                                          val writer:    BlockWriter[F],
                                          val inMemoryMapSearchMap: InMemoryMapSearch[F]
                                  )(implicit F: Sync[F], T: Concurrent[F], C: ContextShift[F], A: Async[F])
  extends ArKiApi[F]{
  @volatile var cmap                   = new ConcurrentSkipListMap[Long, PayloadBuffer]()
  private final val currentPageOffSet  = new AtomicInteger(0)
  private final val pageCounter        = new AtomicInteger(0)
  private final val maxAllowedBytes    = new AtomicInteger(0)

  @inline final def getCurrentPageOffSet = currentPageOffSet.get()
  @inline final def getCurrentPage       = pageCounter.get()

  final def add0(
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
        F.ifM(F.delay(cmap.put(key, payloadBuffer) == null))(
          F.delay(FrozenInMemoryBlock.EMPTY),
          // entry exist lets reset counters
          F.delay(maxAllowedBytes.addAndGet(-entrySizeInBytes))
            *> F.delay(currentPageOffSet.addAndGet(-payloadSize)) *> F.delay(FrozenInMemoryBlock.EMPTY)
        ),
        F.suspend {
          F.ifM(F.delay(maxAllowedBytes.get() + config.pageSize > config.maxAllowedBlockSize))(
            resetCounters(key, payloadBuffer, guard, entrySizeInBytes, payloadSize),
            F.ifM(F.delay(cmap.put(key, payloadBuffer) == null))(
              F.suspend {
                println(s"MAX+PAGESIZE CMAP=${cmap.size()} MaxAllowedBytes=${maxAllowedBytes.get()} PageCounter=${pageCounter.get()} CurrentPageOff=${currentPageOffSet.get()}")
                currentPageOffSet.set(0)
                currentPageOffSet.addAndGet(payloadSize)
                pageCounter.incrementAndGet()
                F.delay(FrozenInMemoryBlock.EMPTY)
              },
              // entry exist lets reset counters
              F.delay(maxAllowedBytes.addAndGet(-entrySizeInBytes))
                *> F.delay(currentPageOffSet.addAndGet(-payloadSize)) *> F.delay(FrozenInMemoryBlock.EMPTY)
            )
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
  final private def resetCounters(
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
            val addMap: F[Unit] = inMemoryMapSearchMap.putMapInBuffer(cmap)
            pageCounter.set(0)
            currentPageOffSet.set(0)
            maxAllowedBytes.set(0)
            val offset = currentPageOffSet.addAndGet(payloadSize)
            maxAllowedBytes.addAndGet(entrySizeInBytes)
            cmap = new ConcurrentSkipListMap[Long, PayloadBuffer]()

            F.delay(cmap.put(key, payloadBuffer)) *> addMap *> F.delay(block)
          }
      }
    }
  }

  final def put0(key: ByteBuffer, value: ByteBuffer, guard: F[Semaphore[F]]): F[FrozenInMemoryBlock] = {
    for {
      genratedKey  <- F.delay {
        hasher.hash(key.array())
      }
      (k, v) = makeByteVectors(key, value)
      fimb   <- add0(genratedKey.underlying, PayloadBuffer.fromKeyValue(k, v), guard)
    } yield fimb

  }

  final def processFrozenMemoryBlock(fimb: FrozenInMemoryBlock): F[Unit] = {
    F.ifM(F.delay(fimb == FrozenInMemoryBlock.EMPTY))(
      F.unit,
      {
        val buildF: F[Unit] = for {
          fiber <- T.start(writer.build(fimb.map, fimb.totalPages))
          bwr   <- fiber.join
          _     <- inMemoryMapSearchMap.remove(bwr.minKey.underlying, bwr.maxKey.underlying)
        } yield ()
        F.guarantee(C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(buildF))(
          Async.shift[F](IOThreadFactory.nonBlockingPool.executionContext)(A)
        )
      }
    )
  }

  final def put(key: ByteBuffer, value: ByteBuffer, guard: F[Semaphore[F]]): F[Unit] = {
    for {
      frozenBlock <- put0(key, value, guard)
      _           <- processFrozenMemoryBlock(frozenBlock)
    } yield ()
  }

  private final def makeByteVectors(k: ByteBuffer, v: ByteBuffer): (KeyByteVector, ValueByteVector)= {
    (KeyByteVector(ByteVector.view(k), k.capacity()), ValueByteVector(ByteVector.view(v), v.capacity()))
  }

  final def get(key: ByteBuffer): F[Search.Result] = {
    val generatedKey = hasher.hash(key.array())
    val payload =  cmap.get(generatedKey.underlying)
    F.ifM(F.delay(payload == null))(
      {
        val  (k, v)= PayloadBuffer.toKeyValueByteVector(payload.underlying)
        val result: Search.Result = Right((k.toArray, v.toArray))
        F.delay(result)
      },
      inMemoryMapSearchMap.searchKey(generatedKey.underlying).flatMap {
        optPayLoadBuffer =>
          optPayLoadBuffer.map {
            payloadBuffer =>
              val (k,v) = PayloadBuffer.toKeyValueByteVector(payloadBuffer.underlying)
              val result: Search.Result = Right((k.toArray,  v.toArray))
              F.delay(result)
          }.getOrElse {
            search.get(generatedKey)
          }
      }
    )

  }
}

