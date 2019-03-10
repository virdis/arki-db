
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
import java.util

import cats.effect.{Concurrent, IO, Sync}
import cats.effect.concurrent.Semaphore
import com.virdis.BaseSpec
import com.virdis.hashing.Hasher
import com.virdis.inmemory.InMemoryBlock
import com.virdis.models.{FrozenInMemoryBlock, GeneratedKey}
import com.virdis.utils.Config
import net.jpountz.xxhash.XXHash64

import scala.collection.mutable
import scala.util.Random

class BlockWriterSpec extends BaseSpec {
  implicit val hasher = implicitly[Hasher[XXHash64]]
  implicit val concurrentIO = Concurrent[IO]
  implicit val semaphore = Semaphore[IO](1)
  implicit val syc = Sync[IO]
  class Fixture {
    val config = new Config(
      pageSize = 64,
      blockSize = 640,
      bloomFilterSize = 0,
      footerSize = 0
    )
    val random = new Random()
    def genBytes(size: Int) = {
      val array = new Array[Byte](size)
      random.nextBytes(array)
      array
    }

    def buildFixedLenFrozenMap(
                     imb: InMemoryBlock[IO, XXHash64],
                     sem: IO[Semaphore[IO]]
                     ) = {
      val map = new mutable.HashMap[GeneratedKey, Array[Byte]]()
      def go(fimb: FrozenInMemoryBlock, counter: Int): (FrozenInMemoryBlock, mutable.HashMap[GeneratedKey, Array[Byte]]) = {
        if (!fimb.isEmpty) (fimb, map)
        else {
          val keyBytes = ByteBuffer.allocate(4)
          val valueBytes = ByteBuffer.allocate(4)
          keyBytes.putInt(counter)
          valueBytes.putInt(counter)
          println(s"BUILD FROZENMAP KEY=${keyBytes} VALUE=${valueBytes}")
          val generatedKey = imb.hasher.hash(keyBytes.duplicate())
          map.put(generatedKey, keyBytes.array())
          println(s"BUILD FROZENMAP KEY=${keyBytes} VALUE=${valueBytes}")
          go(imb.add(keyBytes, valueBytes, sem).unsafeRunSync(), counter+1)
        }
      }
      go(FrozenInMemoryBlock.EMPTY, 0)
    }
    val imb = new InMemoryBlock[IO, XXHash64](config, hasher) {}
  }

  it should "build Block from FrozenInMemoryBlock" in {
    val f = new Fixture
    import f._
    val (fimb, orignalMap) = buildFixedLenFrozenMap(imb, semaphore)
    println(s"FIMB totalPages=${fimb.totalPages}")
    val br = imb.blockWriter.build(fimb.map, fimb.totalPages).unsafeRunSync()
    val indexByteBuffer = br.indexByteBuffer
    indexByteBuffer.underlying.flip()
    println(s"INDEXBYTEBUFFER=${indexByteBuffer}")
    while(indexByteBuffer.underlying.hasRemaining) {
      val (gk, pg, off) = indexByteBuffer.getIndexElement
      println(s"Page=${pg} offset=${off}")

      br.pages.pages(pg.underlying).position(off.underlying)
      val pbKeySize = br.pages.pages(pg.underlying).getShort
      val key = new Array[Byte](pbKeySize)
      br.pages.pages(pg.underlying).get(key)
    //  println(s"Key=${ByteBuffer.wrap(key).getInt()}")
      val dataFromOrignalMap = orignalMap.get(gk).getOrElse(Array.empty[Byte])
      val result = util.Arrays.equals(dataFromOrignalMap, key)
      if (result) println(s"Result=${result}")
    }
    assert(1==1)
  }
}


















