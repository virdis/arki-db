
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

package com.virdis.io

import java.nio.ByteBuffer
import java.util.Random

import cats.Applicative
import cats.effect.{Concurrent, ContextShift, IO, Sync}
import cats.effect.concurrent.Semaphore
import cats.kernel.Hash
import com.virdis.BaseSpec
import com.virdis.hashing.Hasher
import com.virdis.inmemory.InMemoryBlock
import com.virdis.models.{FrozenInMemoryBlock, GeneratedKey, PayloadBuffer}
import com.virdis.threadpools.IOThreadFactory
import com.virdis.utils.{Config, Constants}
import net.jpountz.xxhash.XXHash64
import org.scalacheck.Gen

import scala.concurrent.Future

class InMemoryBlockSpec extends BaseSpec {
  implicit val hasher = implicitly[Hasher[XXHash64]]
  implicit val ioShift: ContextShift[IO] = IO.contextShift(IOThreadFactory.blockingIOPool.executionContext)
  implicit val concurrentIO = Concurrent[IO]
  implicit val semaphore = Semaphore[IO](1)
  implicit val syc = Sync[IO]

  class Fixture {
    val config = new Config(
      blockSize = Constants.SIXTY_FOUR_MB_BYTES / 4 ,// 16 MB
      bloomFilterSize = 262144 // 256
    )
    def buildImMemoryBlock(config: Config) =
      new InMemoryBlock[IO, XXHash64](config, hasher) {}

    val imb = buildImMemoryBlock(config)
    val random = new Random()

    def generateMapData(dataSize: Int = 81): (GeneratedKey, ByteBuffer, ByteBuffer) = {
      val array = new Array[Byte](dataSize)
      random.nextBytes(array)
      val gKey  = hasher.hash(ByteBuffer.wrap(array))
      val key   = ByteBuffer.wrap(array)
      val value = ByteBuffer.wrap(array)
      (gKey, key, value)
    }
    def buildMap(
                  imb: InMemoryBlock[IO, XXHash64],
                  semaphore: IO[Semaphore[IO]],
                  noOfPages: Int = config.pagesFromAllowBlockSize - 1
                ) = {

      def go(fimb: FrozenInMemoryBlock): FrozenInMemoryBlock = {
        if (!fimb.isEmpty) fimb
        else {
          val (gKey, key, value) = generateMapData()
          go(imb.add0(gKey.underlying,
            PayloadBuffer.fromKeyValue(key, value), semaphore).unsafeRunSync())
        }
      }
      go(FrozenInMemoryBlock.EMPTY)
    }

    val imbPg512 = buildImMemoryBlock(
      new Config(
        blockSize = Constants.SIXTY_FOUR_MB_BYTES / 64,
        pageSize = Constants.PAGE_SIZE / 8, // 512 K
        bloomFilterSize = 0,
        footerSize =  0
      )
    )
  }

  it should "build map with correct size" in {
    val f = new Fixture
    import f._
    val fimb = buildMap(imb, semaphore)
    assert(!fimb.isEmpty && imb.cmap.isEmpty)
  }
  it should "update maps" in {
    val f = new Fixture
    import f._
    val fimb = buildMap(imb, semaphore)
    val list = List.fill(10)(generateMapData())
    list.foreach(data => imb.add0(data._1.underlying, PayloadBuffer.fromKeyValue(data._2, data._3), semaphore).unsafeRunSync())
    assert(!fimb.isEmpty && !imb.cmap.isEmpty && (imb.cmap.size() == 10))
  }
  it should "update the pages" in {
    val f = new Fixture
    import f._
    val list = List.fill(5)(generateMapData())
    list.foreach(data => imbPg512.add0(data._1.underlying, PayloadBuffer.fromKeyValue(data._2, data._3), semaphore).unsafeRunSync())
    assert(imbPg512.getCurrentPage == 2)

  }
}
