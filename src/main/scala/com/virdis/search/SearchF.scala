
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

package com.virdis.search

import java.nio.ByteBuffer

import cats.effect.{ContextShift, Sync}
import com.virdis.hashing.Hasher
import com.virdis.search.inmemory.{InMemoryCacheF, RangeF}
import net.jpountz.xxhash.XXHash64

final class SearchF[F[_]](
                         rangeF:    RangeF[F],
                         inmemoryF: InMemoryCacheF[F],
                         bisearch:  BlockIndexSearch[F]
                         )(implicit F: Sync[F], C: ContextShift[F]) {
  val hasher: Hasher[XXHash64] = Hasher.xxhash64
  // search
/*  def get(key: ByteBuffer) = {
    F.flatMap(F.delay(hasher.hash(key))) {
      genKey =>
        F.flatMap(rangeF.get(genKey.underlying)) {
          optRangeValue =>
            optRangeValue.map {
              rangeV =>
                ???
            }
        }
    }
  }*/
}
