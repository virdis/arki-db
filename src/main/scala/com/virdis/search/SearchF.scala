
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

import cats.effect.{ContextShift, Sync}
import com.virdis.bloom.BloomFilterF
import com.virdis.hashing.Hasher
import com.virdis.models.{ArKiResult, BloomFilterError, CacheKeyNotFound, Footer, GeneratedKey, InMemoryRangeSearch, IndexError, SearchResult}
import com.virdis.search.Search.{KVBuffers, Result}
import com.virdis.search.inmemory.{SearchCaches, RangeF, Range}
import com.virdis.utils.{Config, Constants, Utils}
import net.jpountz.xxhash.XXHash64

final class SearchF[F[_]](
                           rangeF:            Range[F],
                           inmemoryF:         SearchCaches[F],
                           blockIndexSearch:  IndexSearch[F],
                           config:            Config
                         )(implicit F: Sync[F]) extends Search[F] {
  final val hasher: Hasher[XXHash64]  = Hasher.xxhash64
  final val bloomFilter: BloomFilterF = new BloomFilterF(config.bloomFilterBits, config.bloomFilterHashes)
  // search
  final def get(key: GeneratedKey): F[Result] = {
    F.flatMap(rangeF.get(key.underlying)) {
      optRangeValue =>
        optRangeValue.map {
          rangeV =>
            F.flatMap(searchBloomFilter(key, rangeV)) {
              _.fold[F[Result]](
                err => F.delay(Left[ArKiResult, KVBuffers](err)),
                cacheKey => {
                  F.flatMap(searchIndex(key, rangeV.footer, cacheKey)) {
                    searchResult =>
                      F.ifM(F.delay(searchResult == SearchResult.NOT_FOUND))(
                        F.delay(Left[ArKiResult, KVBuffers](IndexError)),
                        F.flatMap(searchData(searchResult, cacheKey)) {
                          res =>
                            F.delay(Right[ArKiResult, KVBuffers](res))
                        }
                      )
                  }
                }
              )
            }
        }.getOrElse(F.delay[Result](Left[ArKiResult, KVBuffers](CacheKeyNotFound)))
    }
  }

  final def searchBloomFilter(generatedKey: GeneratedKey, rangeFValue: InMemoryRangeSearch): F[Either[ArKiResult, String]] = {
    F.flatMap(F.delay(Utils.buildKey(rangeFValue.footer))) {
      key =>
        F.flatMap(inmemoryF.bloomFilterCache.get(key, SearchCaches.defaultBFilterFetch)) {
          bitVec =>
            F.ifM(F.delay(bloomFilter.contains(generatedKey)))(
              F.delay(Right(key)),
              F.delay(Left(BloomFilterError))
            )
        }
    }
  }

  final def searchIndex(generatedKey: GeneratedKey, footer: Footer, key: String): F[SearchResult] = {
    F.flatMap(inmemoryF.indexCache.get(key, SearchCaches.defaultBBCacheFetch)) {
      indexByteBuff =>
        F.flatMap(F.delay(Utils.duplicateAndFlipBuffer(indexByteBuff))) {
          ibb =>
            blockIndexSearch.binarySearch(ibb, generatedKey.underlying,
              Constants.INDEX_KEY_SIZE ,0, footer.noOfKeysInIndex.underlying)
        }
    }
  }

  final def searchData(searchResult: SearchResult, key: String): F[KVBuffers] = {
    F.flatMap(inmemoryF.dataCache.get(key, SearchCaches.defaultBBCacheFetch)) {
      dataByteBuff =>
        F.flatMap(F.delay(Utils.duplicateAndFlipBuffer(dataByteBuff))) {
          dbb =>
            val address = (config.pageSize * searchResult.page.underlying) + searchResult.offSet.underlying
            val (key, value) = Utils.kvByteBuffers(address, dbb)
            F.delay(Tuple2(key, value))
        }
    }
  }

}
