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

import java.nio.ByteBuffer

import cats.effect.{ContextShift, Sync}
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.virdis.utils.{BFCache, CacheKind, Config, DataCache, IndexCache}
import scodec.bits.BitVector



// TODO Build Caches from Disk
final class InMemoryCacheF[F[_]](config: Config)(implicit F: Sync[F], C: ContextShift[F]) {
  import InMemoryCacheF._

  final val bloomFilterCache = new CacheF[F, String, BitVector](config, BFCache)
  final val indexCache       = new CacheF[F, String, ByteBuffer](config, IndexCache)
  final val dataCache        = new CacheF[F, String, ByteBuffer](config, DataCache)
}

object InMemoryCacheF {

  final class CacheF[F[_], K <: AnyRef, V <: AnyRef](config: Config, kind: CacheKind)(implicit F: Sync[F], C: ContextShift[F]) {
    private final val cacheF: Cache[K, V] = Caffeine.newBuilder().maximumSize(config.cacheSize(kind)).build()
    def put(key: K, value: V): F[Unit] = F.delay(cacheF.put(key, value))
    def get(key: K, f: java.util.function.Function[K, V]): F[V] = F.delay(cacheF.get(key, f))
  }


}