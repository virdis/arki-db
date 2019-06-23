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

package com.virdis.search.inmemory

import java.nio.{Buffer, ByteBuffer}
import java.util.function

import cats.effect.{ContextShift, Sync}
import com.github.benmanes.caffeine.cache.{Cache, Caffeine, RemovalCause, RemovalListener}
import com.virdis.bloom.BloomFilterF
import com.virdis.threadpools.IOThreadFactory
import com.virdis.utils.{BFCache, CacheKind, Config, DataCache, IndexCache, Utils}

// TODO Build Caches from Disk
final class InMemoryCacheF[F[_]](config: Config)(implicit F: Sync[F], C: ContextShift[F]) {

  final val bloomFilterCache = CacheF.bloomFilterCache[F, String, BloomFilterF](config, BFCache)
  final val indexCache       = CacheF.byteBufferCache[F, String, ByteBuffer](config, IndexCache)
  final val dataCache        = CacheF.byteBufferCache[F, String, ByteBuffer](config, DataCache)
}

object InMemoryCacheF {
  // No need to create instances everytime use defaults instead
  // TODO use Footer to load Caches from Disk
  final val defaultBFilterFetch = new function
  .Function[String, BloomFilterF] {
    override def apply(t: String): BloomFilterF = new BloomFilterF(0,0)
  }
  private final val bbEmpty = ByteBuffer.allocate(0)
  final val defaultBBCacheFetch = new function.Function[String, ByteBuffer] {
    override def apply(t: String): ByteBuffer = bbEmpty
  }
}

trait CacheF[F[_], K, V] {
  def config: Config
  def cacheKind: CacheKind
  def cacheF: Cache[K,V]
  def put(key: K, value: V)(implicit F: Sync[F]): F[Unit] = F.delay(cacheF.put(key, value))
  def get(key: K, f: java.util.function.Function[K, V])(implicit F: Sync[F]) = F.delay(cacheF.get(key, f))
}

object CacheF {
  def bloomFilterCache[F[_], K <: AnyRef, V <: BloomFilterF](
                                                           cfg: Config,
                                                           ckind: CacheKind
                                                         )(implicit F: Sync[F], C: ContextShift[F]): CacheF[F, K, V] =
    new CacheF[F, K, V] {
      override final val config:Config = cfg

      override final val cacheKind: CacheKind = ckind

      override final val cacheF: Cache[K, V] = Caffeine
        .newBuilder()
        .maximumSize(config.cacheSize(cacheKind))
        .build()
  }

  def byteBufferCache[F[_], K <: AnyRef, V <: Buffer](
                                                       cfg: Config,
                                                       ckind: CacheKind
                                                     )(implicit F: Sync[F], C: ContextShift[F]): CacheF[F, K, V] =
    new CacheF[F, K, V] {
      override final val config: Config = cfg

      override final val cacheKind: CacheKind = ckind

      override final val cacheF: Cache[K, V] = Caffeine
        .newBuilder()
        .maximumSize(config.cacheSize(cacheKind))
        .removalListener(new RemovalListener[K,V] {
          override def onRemoval(key: K, value: V, cause: RemovalCause): Unit = {
            C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(Utils.freeDirectBuffer(value)(F, C))
          }
        })
        .build()
    }
}