
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

import java.io.RandomAccessFile
import java.nio.{ByteBuffer, ByteOrder, MappedByteBuffer}
import java.nio.channels.FileChannel

import cats.implicits._
import cats.effect.{Async, ContextShift, Resource, Sync}
import com.virdis.bloom.BloomFilterF
import com.virdis.models._
import com.virdis.search.inmemory.{SearchCaches}
import com.virdis.threadpools.IOThreadFactory
import com.virdis.utils.{Config, Constants, Utils}
import scodec.bits.{BitVector, ByteOrdering, ByteVector}
import cats.collections.{Range => CatsRange}
import com.virdis.search.inmemory._

final class BlockWriterF[F[_]](
                                config:  Config,
                                inmemoryF: SearchCaches[F],
                                range: Range[F]
                             )(implicit F: Sync[F], C: ContextShift[F], A: Async[F]) extends BlockWriter[F] {

  type Position = Int
  type Size     = Int // Size in Bytes
  type NoOfKeys = Int

  private final def build0(
                            map: java.util.NavigableMap[Long, PayloadBuffer],
                            totalPages: Int
                          ): BlockWriterResult = {
    val keySet          = map.navigableKeySet()
    val iterator        = keySet.iterator()
    val indexBuffer     = new IndexByteBuffer(ByteBuffer.allocateDirect(keySet.size() * config.indexKeySize))
    val dataBufferSize  = totalPages * config.pageSize
    val dataBuffer      = ByteBuffer.allocateDirect(dataBufferSize)
    val pages           = new PageAlignedDataBuffer(config.pageSize, dataBuffer)
    val bloomFilter     = new BloomFilterF(config.bloomFilterBits, config.bloomFilterHashes)
    while(iterator.hasNext) {
      val key: Long         = iterator.next()
      val pb: PayloadBuffer = map.get(key)
      // we don't need bound check here since the map will be under maxAllowedBlockSize
      val (page,offSet) = pages.add(pb)
      val generatedKey  = GeneratedKey(key)
      indexBuffer.add(generatedKey, page, offSet)
      if (config.isBloomEnabled) bloomFilter.put(generatedKey)
    }
    BlockWriterResult(
      pages,
      indexBuffer,
      keySet.size(),
      MinKey(map.firstEntry().getKey),
      MaxKey(map.lastEntry().getKey),
      bloomFilter
    )
  }

  final def build(map: java.util.NavigableMap[Long, PayloadBuffer], pages: Int): F[BlockWriterResult] = {
    F.guarantee {
      C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(F.delay(build0(map, pages)))
    }(Async.shift[F](IOThreadFactory.blockingIOPool.executionContext)(A))
  }

  def writeData(mappedByteBuffer: MappedByteBuffer, pageAlignedDataBuffer: PageAlignedDataBuffer): (Position, Size) = {
    val positionData = mappedByteBuffer.position()
    val dupPageAlignedDataBuffer = Utils.duplicateAndFlipBuffer(pageAlignedDataBuffer.buffer)
    mappedByteBuffer.put(dupPageAlignedDataBuffer)
    (positionData, dupPageAlignedDataBuffer.position())
  }

  def writeIndex(
                  mappedByteBuffer: MappedByteBuffer,
                  indexByteBuffer: IndexByteBuffer,
                  noOfKeys: NoOfKeys
                ): (Position, NoOfKeys) = {
    val indexPosition = mappedByteBuffer.position()
    val dupIndexBuffer = Utils.duplicateAndFlipBuffer(indexByteBuffer.underlying)
    mappedByteBuffer.put(dupIndexBuffer)
    (indexPosition, noOfKeys)
  }

  def writeBloomFilter(mappedByteBuffer: MappedByteBuffer, bloomFilter: Array[Int]): (Position, Size) = {
    val bfilterPosition = mappedByteBuffer.position()
    val intBB = ByteBuffer.allocateDirect(bloomFilter.size * 4) // TODO clean up ByteBuffer
    bloomFilter.foreach(i => intBB.putInt(i))
    val dupIBB = Utils.duplicateAndFlipBuffer(intBB)
    mappedByteBuffer.put(dupIBB)
    (bfilterPosition, dupIBB.position()) // no of bytes written
  }

  def writeFooter(mappedByteBuffer: MappedByteBuffer, footer: Footer, config: Config): ByteBuffer = {
    mappedByteBuffer.position(config.blockSize - Constants.FOOTER_SIZE)
    val ts               = ByteVector.fromLong(footer.timeStamp.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val minKey           = ByteVector.fromLong(footer.minKey.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val maxKey           = ByteVector.fromLong(footer.maxKey.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val indexOffset      = ByteVector.fromLong(footer.indexStartOffSet.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val noOfKeys         = ByteVector.fromInt(footer.noOfKeysInIndex.underlying, Constants.INT_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val bfOffset         = ByteVector.fromLong(footer.bfilterStartOffset.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val blockNo          = ByteVector.fromInt(footer.blockNumber.underlying, Constants.INT_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val dataBufferOffset = ByteVector.fromLong(footer.dataBufferOffSet.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val dataBufferSize   = ByteVector.fromInt(footer.dataBufferSize.underlying, Constants.INT_SIZE_IN_BYTES, ByteOrdering.BigEndian)

    mappedByteBuffer.put(ts.toArray)
    mappedByteBuffer.put(minKey.toArray)
    mappedByteBuffer.put(maxKey.toArray)
    mappedByteBuffer.put(dataBufferOffset.toArray)
    mappedByteBuffer.put(dataBufferSize.toArray)
    mappedByteBuffer.put(indexOffset.toArray)
    mappedByteBuffer.put(noOfKeys.toArray)
    mappedByteBuffer.put(bfOffset.toArray)
    mappedByteBuffer.put(blockNo.toArray)
    mappedByteBuffer
  }

  def readFooter(mappedByteBuffer: MappedByteBuffer): Footer = {
    val tsArray = new Array[Byte](Constants.LONG_SIZE_IN_BYTES)
    mappedByteBuffer.get(tsArray)
    val minKey = new Array[Byte](Constants.LONG_SIZE_IN_BYTES)
    mappedByteBuffer.get(minKey)
    val maxKey = new Array[Byte](Constants.LONG_SIZE_IN_BYTES)
    mappedByteBuffer.get(maxKey)
    val dataBufferOffSetArray = new Array[Byte](Constants.LONG_SIZE_IN_BYTES)
    mappedByteBuffer.get(dataBufferOffSetArray)
    val dataBufferSizeArray = new Array[Byte](Constants.INT_SIZE_IN_BYTES)
    mappedByteBuffer.get(dataBufferSizeArray)
    val indexOffSetArray = new Array[Byte](Constants.LONG_SIZE_IN_BYTES)
    mappedByteBuffer.get(indexOffSetArray)
    val noOfKeyArray = new Array[Byte](Constants.INT_SIZE_IN_BYTES)
    mappedByteBuffer.get(noOfKeyArray)
    val bfOffSetArray = new Array[Byte](Constants.LONG_SIZE_IN_BYTES)
    mappedByteBuffer.get(bfOffSetArray)
    val blockNoArray = new Array[Byte](Constants.INT_SIZE_IN_BYTES)
    mappedByteBuffer.get(blockNoArray)

    Footer(
      Ts(ByteBuffer.wrap(tsArray).order(ByteOrdering.BigEndian.toJava).getLong),
      MinKey(ByteBuffer.wrap(minKey).order(ByteOrdering.BigEndian.toJava).getLong),
      MaxKey(ByteBuffer.wrap(maxKey).order(ByteOrdering.BigEndian.toJava).getLong),
      DataBufferOffSet(ByteBuffer.wrap(dataBufferOffSetArray).order(ByteOrdering.BigEndian.toJava).getLong),
      DataBufferSize(ByteBuffer.wrap(dataBufferSizeArray).order(ByteOrdering.BigEndian.toJava).getInt),
      IndexStartOffSet(ByteBuffer.wrap(indexOffSetArray).order(ByteOrdering.BigEndian.toJava).getLong),
      NoOfKeysInIndex(ByteBuffer.wrap(noOfKeyArray).order(ByteOrdering.BigEndian.toJava).getInt),
      BFilterStartOffset(ByteBuffer.wrap(bfOffSetArray).order(ByteOrdering.BigEndian.toJava).getLong),
      BlockNumber(ByteBuffer.wrap(blockNoArray).order(ByteOrdering.BigEndian.toJava).getInt)
    )
  }

  def updateCaches(bwr: BlockWriterResult, footer: Footer, fileName: String): F[Unit] = {
    val key = Utils.buildKey(footer)
    val rangeFValue = InMemoryRangeSearch(CatsRange(footer.minKey.underlying, footer.maxKey.underlying), fileName, footer)
    range.add(rangeFValue)
    inmemoryF.bloomFilterCache.put(key, bwr.bloomFilter)
    inmemoryF.indexCache.put(key, Utils.duplicateAndFlipBuffer(bwr.indexByteBuffer.underlying))
    inmemoryF.dataCache.put(key, Utils.duplicateAndFlipBuffer(bwr.underlying.buffer))

  }


  def write(blockWriterResult: BlockWriterResult): F[String] = {
    val fileF: FileF = new FileF(config)
    def makeFile(file: RandomAccessFile): Resource[F, RandomAccessFile] = Resource.fromAutoCloseable(F.delay(file))
    def makeChannel(channel: FileChannel): Resource[F, FileChannel]     = Resource.fromAutoCloseable(F.delay(channel))

    makeFile(fileF.rFile).use {
      file =>
        makeChannel(file.getChannel).use {
          channel =>
            val mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, config.blockSize)

            F.flatMap(C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(F.delay(writeData(mappedByteBuffer, blockWriterResult.underlying)))){
              case (dataPos, dataSize) =>
                F.flatMap(C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(F.delay(writeIndex(mappedByteBuffer,
                  blockWriterResult.indexByteBuffer, blockWriterResult.totalNoKeys)))){
                  case (indexPos, noOfKeys) =>
                    F.flatMap(C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(F.delay(writeBloomFilter(mappedByteBuffer, blockWriterResult.bloomFilter.bloomfilter)))){
                      case (bfPos, bfSize) =>
                        F.delay {
                          val footer = Footer(
                            Ts(System.nanoTime()),
                            blockWriterResult.minKey,
                            blockWriterResult.maxKey,
                            DataBufferOffSet(dataPos),
                            DataBufferSize(dataSize),
                            IndexStartOffSet(indexPos),
                            NoOfKeysInIndex(noOfKeys),
                            BFilterStartOffset(bfPos),
                            BlockNumber(0)
                          )
                          writeFooter(mappedByteBuffer, footer, config)
                          updateCaches(blockWriterResult, footer, fileF.name)
                        }
                    }
                }
            } *> C.evalOn(IOThreadFactory
              .blockingIOPool.executionContext)(F.delay(mappedByteBuffer.force())) *> F.delay(fileF.name)
        }
    }
  }

}
