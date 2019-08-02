
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

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths}
import java.util

import cats.effect.{Concurrent, IO, Sync}
import cats.effect.concurrent.Semaphore
import com.virdis.BaseSpec
import com.virdis.bloom.BloomFilterF
import com.virdis.hashing.Hasher
import com.virdis.inmemory.InMemoryBlock
import com.virdis.models._
import com.virdis.search.{BlockIndexSearchF, IndexSearch, SearchF}
import com.virdis.search.inmemory.{InMemoryMapSearchF, RangeF, SearchCaches}
import com.virdis.utils.{Config, Constants, Utils}
import net.jpountz.xxhash.XXHash64
import org.scalacheck.Gen
import scodec.bits.{BitVector, ByteVector}

import scala.collection.mutable
import scala.util.Random

class BlockWriterFSpec extends BaseSpec {
  implicit val hasher = implicitly[Hasher[XXHash64]]
  implicit val concurrentIO = Concurrent[IO]
  implicit val semaphore = Semaphore[IO](1)
  implicit val syc = Sync[IO]

  class Fixture {
    val config = new Config(
      pageSize = 64,
      blockSize = 640,
      footerSize = 0
    )
    val config128 = new Config(
      pageSize = 128,
      blockSize = 1280,
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
          //TODO REPLACE BUFFER WITH ARRAYS
          val keyBytes = ByteBuffer.allocate(4)
          val valueBytes = ByteBuffer.allocate(4)
          val bytesfromKey = ByteBuffer.allocate(4)
          keyBytes.putInt(counter)
          valueBytes.putInt(counter)
          keyBytes.flip()
          valueBytes.flip()
          bytesfromKey.putInt(counter)
          keySet.add(ByteBuffer.wrap(keyBytes.array()))
          bytesfromKey.flip()
          val genKey = imb.hasher.hash(bytesfromKey.duplicate().array())
          genKeySet.add(genKey)
          go(imb.put0(keyBytes, valueBytes, sem).unsafeRunSync(), counter + 1, genKeySet, keySet)
        }
      }

      go(FrozenInMemoryBlock.EMPTY, 0, new mutable.HashSet[GeneratedKey](), new mutable.HashSet[ByteBuffer]())
    }
    val rangeF                = new RangeF[IO]
    val inmemoryF             = new SearchCaches[IO](config)
    val blockIndexSearchF     = new BlockIndexSearchF[IO]()
    val searchF               = new SearchF[IO](rangeF, inmemoryF, blockIndexSearchF, config)
    val writerF               = new BlockWriterF[IO](config, inmemoryF, rangeF)
    val inmemoryMapSearch     = new InMemoryMapSearchF[IO]()
    val imb                   = new InMemoryBlock[IO, XXHash64](config, searchF, hasher, writerF, inmemoryMapSearch) {}
    val searchF128            = new SearchF[IO](rangeF, inmemoryF, blockIndexSearchF, config128)
    val writerF128            = new BlockWriterF[IO](config128, inmemoryF, rangeF)
    val inmemoryMapSearch128  = new InMemoryMapSearchF[IO]()
    val imb128                = new InMemoryBlock[IO, XXHash64](config128, searchF128, hasher, writerF128, inmemoryMapSearch128) {}

    def frozenMapWithRandomData(
                                 imb: InMemoryBlock[IO, XXHash64],
                                 sem: IO[Semaphore[IO]]
                               ) = {
      val random = new Random()
      def go(fimb: FrozenInMemoryBlock,
             counter: Int,
             set: mutable.Set[GeneratedKey],
             keySet: mutable.Set[ByteBuffer]
            ): (FrozenInMemoryBlock, mutable.Set[GeneratedKey], mutable.Set[ByteBuffer]) = {
        if (!fimb.isEmpty) (fimb, set, keySet)
        else {
          val counter = random.nextInt(26)
          val keySize = 27 - counter
          val valueSize = counter
          val keyBytes = random.nextString(keySize)
          val valueBytes = random.nextString(valueSize)
          val kBuffer = ByteBuffer.wrap(keyBytes.getBytes)
          val keyByteVector = KeyByteVector(ByteVector.view(kBuffer), kBuffer.capacity())
          val vBuffer = ByteBuffer.wrap(valueBytes.getBytes)
          val valueByteVector = ValueByteVector(ByteVector.view(vBuffer), vBuffer.capacity())
          val genKey = imb.hasher.hash(keyBytes.getBytes)
          set.add(genKey)
          keySet.add(ByteBuffer.wrap(keyBytes.getBytes))
          go(imb.add0(genKey.underlying,
            PayloadBuffer.fromKeyValue(
              keyByteVector,
              valueByteVector), sem).unsafeRunSync(), counter + 1, set, keySet)
        }
      }
      go(
        FrozenInMemoryBlock.EMPTY,
        0,
        new mutable.HashSet[GeneratedKey](),
        new mutable.HashSet[ByteBuffer]()
      )
    }
    // TODO : Remove hardcoded values
    def buildBlockWriterResult = {
      // build block total of 4 MB // 4194304 bytes
      val config = new Config(blockSize = 4194304, dataDirectory = ".")
      val pgadbBV = ByteBuffer.allocate(1048576)
      (0 to 500).foreach{i => pgadbBV.putInt(i)}
      val pgAD = new PageAlignedDataBuffer(100, pgadbBV)
      val indxBV = ByteBuffer.allocate( 500 * Constants.INDEX_KEY_SIZE)
      (1 to 500).foreach{i =>
        indxBV.putLong(i.toLong); indxBV.putInt(i); indxBV.putInt(i)}
      val ibb = new IndexByteBuffer(indxBV)
      val bf = new BloomFilterF(100, 3)
      (BlockWriterResult(pgAD, ibb, 500, MinKey(1), MaxKey(100L), bf), config)
    }

  }
  it should "build Block from FrozenInMemoryBlock, should build Index" in {
    val f = new Fixture
    import f._
    val (fimb, set, _) = frozenMapForIndex(imb, semaphore)
    val br = writerF.build(fimb.map, fimb.totalPages).unsafeRunSync()
    val indexByteBuffer = br.indexByteBuffer
    indexByteBuffer.underlying.flip()
    var flag = true
    set.foreach(println)
    while (indexByteBuffer.underlying.hasRemaining) {
      val (gk, pg, off) = indexByteBuffer.getIndexElement
      flag &&= set.contains(gk)
    }
    assert(flag)
  }
  it should "add map to InMemorySearchMap" in {
    val f = new Fixture
    import f._
    val (fimb, set, _) = frozenMapForIndex(imb, semaphore)
    val length = inmemoryMapSearch.internal.flatMap {
      ref =>
        ref.get.map(_.length)
    }.unsafeRunSync()
    assert(length == 2)
  }
  it should "build Block from FrozenInMemoryBlock, should build PageAlignedDataBuffer" in {
    val f = new Fixture
    import f._
    val (fimb, genSet, keySet) = frozenMapForIndex(imb, semaphore)
    val br = writerF.build(fimb.map, fimb.totalPages).unsafeRunSync()
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
  }

  it should "build Block from FrozenInMemoryBlock frozenMapWithRandomData(), should build Index" in {
    val f = new Fixture
    import f._
    val (fimb, set, _) = frozenMapWithRandomData(imb128, semaphore)
    val br = writerF128.build(fimb.map, fimb.totalPages).unsafeRunSync()
    val indexByteBuffer = br.indexByteBuffer
    indexByteBuffer.underlying.flip()
    var flag = true
    while (indexByteBuffer.underlying.hasRemaining) {
      val (gk, pg, off) = indexByteBuffer.getIndexElement
      flag &&= set.contains(gk)
    }
    assert(flag)
  }

  it should "build Block from FrozenInMemoryBlock frozenMapWithRandomData(), should build PageAlignedDataBuffer" in {
    val f = new Fixture
    import f._
    val (fimb, genSet, keySet) = frozenMapWithRandomData(imb128, semaphore)
    val br = writerF128.build(fimb.map, fimb.totalPages).unsafeRunSync()
    val indexByteBuffer = br.indexByteBuffer
    val dataBuffer = br.underlying.buffer
    indexByteBuffer.underlying.flip()
    var flag = true
    while (indexByteBuffer.underlying.hasRemaining) {
      val (gk, pg, off) = indexByteBuffer.getIndexElement
      val address = (config128.pageSize * pg.underlying) + off.underlying
      dataBuffer.position(address)
      val keySize = dataBuffer.getShort
      val key = new Array[Byte](keySize)
      dataBuffer.get(key)
      val valueSize = dataBuffer.getShort()
      val value = new Array[Byte](valueSize)
      dataBuffer.get(value)
      val isDeleted = dataBuffer.get()
      flag &&= keySet.contains(ByteBuffer.wrap(key))
    }
    assert(flag)
  }

  it should "write BlockWriterResult to disk" in {
    val f = new Fixture
    import f._
    val (bwr, cfg) = buildBlockWriterResult
    val rangeF            = new RangeF[IO]
    val inmemoryF         = new SearchCaches[IO](cfg)
    val blockIndexSearchF = new BlockIndexSearchF[IO]()
    val searchF           = new SearchF[IO](rangeF, inmemoryF, blockIndexSearchF, cfg)
    val writerF           = new BlockWriterF[IO](cfg, inmemoryF, rangeF)
    val inmemoryMapSearch = new InMemoryMapSearchF[IO]()
    val imb               = new InMemoryBlock[IO, XXHash64](cfg, searchF, hasher, writerF, inmemoryMapSearch) {}
    val fileName          = writerF.write(bwr).unsafeRunSync()
    println(s"FileName=${fileName}")
    val rafAccess = new RandomAccessFile(cfg.dataDirectory+"/"+fileName, "rw")
    val channel = rafAccess.getChannel.map(FileChannel.MapMode.READ_WRITE, cfg.blockSize - Constants.FOOTER_SIZE, Constants.FOOTER_SIZE)
    val footer = writerF.readFooter(channel)
    val dataBuffer = rafAccess.getChannel.map(FileChannel.MapMode.READ_ONLY, footer.dataBufferOffSet.underlying, footer.dataBufferSize.underlying)
    val indexBB = rafAccess.getChannel.map(FileChannel.MapMode.READ_ONLY,
      footer.indexStartOffSet.underlying, footer.noOfKeysInIndex.underlying * Constants.INDEX_KEY_SIZE)
    bwr.indexByteBuffer.underlying.flip()
    bwr.underlying.buffer.flip()

    val bfBB = rafAccess.getChannel.map(FileChannel.MapMode.READ_ONLY, footer.bfilterStartOffset.underlying, cfg.bloomSizeInBytes)
    Files.deleteIfExists(Paths.get(cfg.dataDirectory + "/" + fileName))
    assert(
      bwr.underlying.buffer.equals(dataBuffer)
        && bwr.indexByteBuffer.underlying.equals(indexBB)
    )
  }
}


















