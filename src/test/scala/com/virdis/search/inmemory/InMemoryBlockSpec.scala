
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

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, IO, Sync}
import com.virdis.BaseSpec
import com.virdis.hashing.Hasher
import com.virdis.inmemory.InMemoryBlock
import com.virdis.io.BlockWriterF
import com.virdis.models.FrozenInMemoryBlock
import com.virdis.search.{BlockIndexSearchF, SearchF}
import com.virdis.utils.{Config, Constants}
import net.jpountz.xxhash.XXHash64
import scodec.bits.{ByteOrdering, ByteVector}

import scala.util.Random

class InMemoryBlockSpec extends BaseSpec {

  implicit val hasher       = implicitly[Hasher[XXHash64]]
  implicit val concurrentIO = Concurrent[IO]
  implicit val semaphore    = Semaphore[IO](1)
  implicit val syc          = Sync[IO]

  class Fixture {
    val config128 = new Config(
      pageSize = 128,
      blockSize = 1280,
      bloomFilterBits = 0,
      bloomFilterHashes = 0,
      footerSize = 0
    )
    val random            = new Random()
    val rangeF            = new RangeF[IO]
    val inmemoryF         = new SearchCaches[IO](config128)
    val blockIndexSearchF = new BlockIndexSearchF[IO]()
    val searchF           = new SearchF[IO](rangeF, inmemoryF, blockIndexSearchF, config128)
    val writerF           = new BlockWriterF[IO](config128, inmemoryF, rangeF)
    val inMemMapSearch    = new InMemoryMapSearchF[IO]()
    val imb128            = new InMemoryBlock[IO, XXHash64](config128, searchF, hasher, writerF, inMemMapSearch) {}
  }

  it should "add key and update counters" in {
    val f = new Fixture
    import f._
    val bv = ByteVector.fromLong(100,8, ByteOrdering.BigEndian)
    val key   = ByteBuffer.wrap(bv.toArray)
    val value = ByteBuffer.wrap(bv.toArray)
    val entrySize = config128.indexKeySize + (2 * Constants.LONG_SIZE_IN_BYTES)

    val fb = imb128.put0(key, value, semaphore).unsafeRunSync()
    assert(fb == FrozenInMemoryBlock.EMPTY
      && imb128.getCurrentPageOffSet == (2 * Constants.LONG_SIZE_IN_BYTES) + 5)
  }
  it should "not update counters multiple times if same key is added" in {
    val f = new Fixture
    import f._
    import cats.implicits._
    val res = (1 to 10).toList.map {
      val bv = ByteVector.fromLong(1000, 8, ByteOrdering.BigEndian)
      val key = ByteBuffer.wrap(bv.toArray)
      val value = ByteBuffer.wrap(bv.toArray)
      _ => imb128.put0(key, value, semaphore)
    }.sequence.unsafeRunSync()
    assert(imb128.getCurrentPageOffSet == (2 * Constants.LONG_SIZE_IN_BYTES) + 5)

  }
}
