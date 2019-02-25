
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

import cats.effect.{Concurrent, ContextShift, IO, Sync}
import cats.effect.concurrent.Semaphore
import cats.kernel.Hash
import com.virdis.BaseSpec
import com.virdis.hashing.Hasher
import com.virdis.inmemory.InMemoryBlock
import com.virdis.models.{FrozenInMemoryBlock, PayloadBuffer}
import com.virdis.threadpools.IOThreadFactory
import com.virdis.utils.{Config, Constants}
import net.jpountz.xxhash.XXHash64
import org.scalacheck.Gen

class InMemoryBlockSpec extends BaseSpec {
  class Fixture {
    def GenString = for {
      s <- Gen.alphaStr.suchThat(_.getBytes.size < 150)
    } yield s
    implicit val hasher = implicitly[Hasher[XXHash64]]
    implicit val ioShift: ContextShift[IO] = IO.contextShift(IOThreadFactory.blockingIOPool.executionContext)
    implicit val concurrentIO = Concurrent[IO]
    implicit val semaphore = Semaphore[IO](1)
    implicit val syc = Sync[IO]
    val config = new Config(
      blockSize = Constants.SIXTY_FOUR_MB_BYTES / 4 ,// 16 MB
      bloomFilterSize = 262144 // 256
    )
    val imb = new InMemoryBlock[IO, XXHash64](config, hasher) {}

    def buildMap(
                  imb: InMemoryBlock[IO, XXHash64],
                  semaphore: Semaphore[IO]
                ) = {
      var fimb = FrozenInMemoryBlock.EMPTY
      while(fimb.map.isEmpty) {
        for {
          data <- GenString
        } yield {
          val gKey  = hasher.hash(ByteBuffer.wrap(data.getBytes))
          val key   = ByteBuffer.wrap(data.getBytes)
          val value = ByteBuffer.wrap(data.getBytes)
          fimb = imb.add0(
            gKey.underlying,
            PayloadBuffer.fromKeyValue(key, value),
            semaphore
          )
        }
      }
      fimb
    }
  }

  it should "build map with correct size" in {
    val f = new Fixture
    import f._
    val result = buildMap(imb, semaphore)
    ???
  }
}
