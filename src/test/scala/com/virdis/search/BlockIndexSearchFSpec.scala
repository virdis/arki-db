
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

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, IO, Sync}
import com.virdis.BaseSpec
import com.virdis.hashing.Hasher
import com.virdis.inmemory.InMemoryBlock
import com.virdis.io.BlockWriterF
import com.virdis.models._
import com.virdis.search.inmemory.{InMemoryCacheF, RangeF}
import com.virdis.utils.{Config, Constants}
import net.jpountz.xxhash.XXHash64
import scodec.bits.{BitVector, ByteOrdering, ByteVector}

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Random

class BlockIndexSearchFSpec extends BaseSpec {

  implicit val hasher = implicitly[Hasher[XXHash64]]
  implicit val concurrentIO = Concurrent[IO]
  implicit val semaphore = Semaphore[IO](1)
  implicit val syc = Sync[IO]

  class Fixture {
    val config128 = new Config(
      pageSize = 128,
      blockSize = 1280,
      bloomFilterBits = 0,
      bloomFilterHashes = 0,
      footerSize = 0
    )
    val random = new Random()
    val rangeF = new RangeF[IO]
    val inmemoryF = new InMemoryCacheF[IO](config128)
    val blockIndexSearchF = new BlockIndexSearchF[IO]()
    val searchF    = new SearchF[IO](rangeF, inmemoryF, blockIndexSearchF, config128)
    val writerF    = new BlockWriterF[IO](config128, inmemoryF, rangeF)
    val imb128     = new InMemoryBlock[IO, XXHash64](config128, searchF, hasher, writerF) {}
    val bis        = new BlockIndexSearchF[IO]()

    def frozenMapForIndex(
                           imb: InMemoryBlock[IO, XXHash64],
                           sem: IO[Semaphore[IO]]
                         ) = {
      def go(fimb: FrozenInMemoryBlock,
             counter: Int,
             genKeySet: mutable.Set[GeneratedKey],
             keySet: mutable.Set[ByteBuffer],
             addedKey: GeneratedKey
            ): (FrozenInMemoryBlock, mutable.Set[GeneratedKey], mutable.Set[ByteBuffer]) = {
        if (!fimb.isEmpty) (fimb, genKeySet - addedKey, keySet)
        else {
          val kbv = BitVector.fromInt(counter, 32, ByteOrdering.BigEndian)

          keySet.add(ByteBuffer.wrap(kbv.toByteArray))
          val genKey = imb.hasher.hash(kbv.toByteArray)
          genKeySet.add(genKey)
          go(imb.put(ByteBuffer.wrap(kbv.toByteArray),
            ByteBuffer.wrap(kbv.toByteArray), sem).unsafeRunSync(), counter + 1, genKeySet, keySet, genKey)
        }
      }

      go(FrozenInMemoryBlock.EMPTY,
        0, new mutable.HashSet[GeneratedKey](),
        new mutable.HashSet[ByteBuffer](), GeneratedKey(0))
    }

  }

  it should "return keys in index" in {
    val f = new Fixture
    import f._
    val (fimb, set, _) = frozenMapForIndex(imb128, semaphore)
    val br = writerF.build(fimb.map, fimb.totalPages).unsafeRunSync()
    val allKeys = set.map(g => g.underlying)
    val result = Future.sequence {
      allKeys.toList.map {
        k =>
          bis.binarySearch(br.indexByteBuffer.underlying, k,
            Constants.INDEX_KEY_SIZE, 0, set.size).unsafeToFuture()
      }
    }
    result.map {
      l =>
        val searchResult = l.map(_.key.underlying).toSet
        assert(
          allKeys.toList.map {
            k =>
              searchResult.contains(k)
          }.forall(_ && true))
    }
  }

}
