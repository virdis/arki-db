
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

package com.virdis.module

import cats.effect._
import com.virdis.api.ArKiApi
import com.virdis.hashing.Hasher
import com.virdis.inmemory.InMemoryBlock
import com.virdis.io.BlockWriterF
import com.virdis.search.{BlockIndexSearchF, IndexSearch, SearchF}
import com.virdis.search.inmemory.{InMemoryMapSearchF, Range, RangeF, SearchCaches}
import com.virdis.threadpools.IOThreadFactory
import com.virdis.utils.Config
import net.jpountz.xxhash.XXHash64

class ArKiModule(config: Config)(implicit R: Rewrite[IO]) {
  final val cfg: Config                             = R.config.getOrElse(config)
  final val range: Range[IO]                        = new RangeF[IO]()
  final val sycIO: Sync[IO]                         = Sync[IO]
  final val blockingContextShift: ContextShift[IO]  = IO.contextShift(IOThreadFactory.blockingIOPool.executionContext)
  final val concurrentF: Concurrent[IO]             = Concurrent[IO](IO.ioConcurrentEffect(blockingContextShift))
  final val asycF: Async[IO] = Async[IO]
  final val inMemSearchCaches: SearchCaches[IO]     = new SearchCaches[IO](cfg)(sycIO, blockingContextShift)
  final val biSearch: IndexSearch[IO]               = new BlockIndexSearchF[IO]()

  final val arki: ArKiApi[IO] = new InMemoryBlock[IO, XXHash64](
    config = R.config.getOrElse(config),
    search = R.search.getOrElse(new SearchF[IO](
      rangeF = range,
      inmemoryF = inMemSearchCaches,
      blockIndexSearch = biSearch,
      config = cfg
    )),
    hasher = R.hasher.getOrElse(Hasher.xxhash64),
    writer = R.writer.getOrElse(new BlockWriterF[IO](
      config = cfg,
      inmemoryF = inMemSearchCaches,
      rangeF = range
    )(sycIO, blockingContextShift, asycF)),
    inMemoryMapSearch = new InMemoryMapSearchF[IO]()
  )(sycIO, concurrentF, blockingContextShift, asycF)
}


