
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
import com.virdis.models.{FrozenInMemoryBlock, GeneratedKey, PayloadBuffer}
import com.virdis.utils.Config
import net.jpountz.xxhash.XXHash64
import org.scalacheck.Gen

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
    val config128 = new Config(
      pageSize = 128,
      blockSize = 1280,
      bloomFilterSize = 0,
      footerSize = 0
    )
    val random = new Random()

    def genBytes(size: Int) = {
      val array = new Array[Byte](size)
      random.nextBytes(array)
      array
    }

    def frozenMapForIndex(
                           imb: InMemoryBlock[IO, XXHash64],
                           sem: IO[Semaphore[IO]]
                         ) = {
      def go(fimb: FrozenInMemoryBlock,
             counter: Int,
             genKeySet: mutable.Set[GeneratedKey],
             keySet: mutable.Set[ByteBuffer]
            ): (FrozenInMemoryBlock, mutable.Set[GeneratedKey], mutable.Set[ByteBuffer]) = {
        if (!fimb.isEmpty) (fimb, genKeySet, keySet)
        else {
          val keyBytes = ByteBuffer.allocate(4)
          val valueBytes = ByteBuffer.allocate(4)
          val bytesfromKey = ByteBuffer.allocate(4)
          val kBytes = ByteBuffer.allocate(4)
          keyBytes.putInt(counter)
          valueBytes.putInt(counter)
          bytesfromKey.putInt(counter)
          kBytes.putInt(counter)
          kBytes.flip()
          keySet.add(kBytes)
          bytesfromKey.flip()
          val genKey = imb.hasher.hash(bytesfromKey)
          genKeySet.add(genKey)
          go(imb.add(keyBytes, valueBytes, sem).unsafeRunSync(), counter + 1, genKeySet, keySet)
        }
      }

      go(FrozenInMemoryBlock.EMPTY, 0, new mutable.HashSet[GeneratedKey](), new mutable.HashSet[ByteBuffer]())
    }

    val imb = new InMemoryBlock[IO, XXHash64](config, hasher) {}

    val imb128 = new InMemoryBlock[IO, XXHash64](config128, hasher) {}

    def frozenMapWithRandomData(
                                 imb: InMemoryBlock[IO, XXHash64],
                                 sem: IO[Semaphore[IO]]
                               ) = {
      val random = new Random()
      def go(fimb: FrozenInMemoryBlock,
             counter: Int,
             set: mutable.Set[GeneratedKey]
            ): (FrozenInMemoryBlock, mutable.Set[GeneratedKey]) = {
        if (!fimb.isEmpty) (fimb, set)
        else {
          val counter = random.nextInt(26)
          val keySize = 27 - counter
          val valueSize = counter
          println(s"KeySize=${keySize} ValueSize=${valueSize}")
          val keyBytes = random.nextString(keySize)
          val valueBytes = random.nextString(valueSize)
          val kBuffer = ByteBuffer.wrap(keyBytes.getBytes)
          val vBuffer = ByteBuffer.wrap(valueBytes.getBytes)
          val genKey = imb.hasher.hash(keyBytes.getBytes)
          set.add(genKey)
          println(s"GenKey=${genKey} KeyBuffer=${kBuffer} ValueBuffer=${vBuffer}")
          go(imb.add0(genKey.underlying,
            PayloadBuffer.fromKeyValue(
              kBuffer,
              vBuffer), sem).unsafeRunSync(), counter + 1, set)
        }
      }
      go(FrozenInMemoryBlock.EMPTY, 0, new mutable.HashSet[GeneratedKey]())
    }


  }

/*

  it should "build Block from FrozenInMemoryBlock, should build Index" in {
    val f = new Fixture
    import f._
    val (fimb, set, _) = frozenMapForIndex(imb, semaphore)
    val br = imb.blockWriter.build(fimb.map, fimb.totalPages).unsafeRunSync()
    val indexByteBuffer = br.indexByteBuffer
    indexByteBuffer.underlying.flip()
    var flag = true
    while (indexByteBuffer.underlying.hasRemaining) {
      val (gk, pg, off) = indexByteBuffer.getIndexElement
      flag &&= set.contains(gk)
    }
    assert(flag)
  }

  it should "build Block from FrozenInMemoryBlock, should build PageAlignedDataBuffer" in {
    val f = new Fixture
    import f._
    val (fimb, genSet, keySet) = frozenMapForIndex(imb, semaphore)
    val br = imb.blockWriter.build(fimb.map, fimb.totalPages).unsafeRunSync()
    val indexByteBuffer = br.indexByteBuffer
    val dataBuffer = br.underlying.buffer
    indexByteBuffer.underlying.flip()
    var flag = true
    while (indexByteBuffer.underlying.hasRemaining) {
      val (gk, pg, off) = indexByteBuffer.getIndexElement
      val address = (config.pageSize * pg.underlying) + off.underlying
      dataBuffer.position(address)
      val keySize = dataBuffer.getShort
      val key = new Array[Byte](keySize)
      dataBuffer.get(key)
      val valueSize = dataBuffer.getShort()
      val value = new Array[Byte](valueSize)
      dataBuffer.get(value)
      val isDeleted = dataBuffer.get()
      flag &&= keySet.contains(ByteBuffer.wrap(key))
      flag &&= keySet.contains(ByteBuffer.wrap(value))
    }
    assert(flag)
  }*/

  it should "build Block from FrozenInMemoryBlock frozenMapWithRandomData(), should build Index" in {
    val f = new Fixture
    import f._
    val (fimb, set) = frozenMapWithRandomData(imb128, semaphore)
    val br = imb128.blockWriter.build(fimb.map, fimb.totalPages).unsafeRunSync()
    val indexByteBuffer = br.indexByteBuffer
    indexByteBuffer.underlying.flip()
    var flag = true
    while (indexByteBuffer.underlying.hasRemaining) {
      val (gk, pg, off) = indexByteBuffer.getIndexElement
      flag &&= set.contains(gk)
    }
    assert(flag)
  }
}


















