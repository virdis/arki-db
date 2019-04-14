
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
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel

import cats.effect.{ContextShift, Effect, Resource, Sync}
import com.virdis.bloom.BloomFilter
import com.virdis.models._
import com.virdis.threadpools.IOThreadFactory
import com.virdis.utils.{Config, Constants, Utils}
import scodec.bits.{BitVector, ByteOrdering, ByteVector}

final class BlockWriter[F[_]](config: Config)(
  implicit F: Sync[F], C: ContextShift[F]
){

  type Position = Int
  type Size     = Int
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
    val pages           = new PageAlignedDataBuffer(totalPages, config.pageSize, dataBuffer)
    var bitVector       = BitVector.fill(config.bloomFilterBits)(false)
    val bloomFilter     = new BloomFilter(config.bloomFilterBits, config.bloomFilterHashes)
    while(iterator.hasNext) {
      val key: Long         = iterator.next()
      val pb: PayloadBuffer = map.get(key)
      // we dont need bound check here since the map will be under maxAllowedBlockSize
      val (page,offSet) = pages.add(pb)
      val generatedKey  = GeneratedKey(key)
      indexBuffer.add(generatedKey, page, offSet)
      if (config.isBloomEnabled) bitVector = bloomFilter.add(bitVector, generatedKey)
    }
    BlockWriterResult(
      pages,
      indexBuffer,
      MinKey(map.firstEntry().getKey),
      MaxKey(map.lastEntry().getKey),
      bitVector
    )
  }

  final def build(map: java.util.NavigableMap[Long, PayloadBuffer], pages: Int)(): F[BlockWriterResult] = {
    C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(F.delay(build0(map, pages)))
  }

  def writeData(mappedByteBuffer: MappedByteBuffer, pageAlignedDataBuffer: PageAlignedDataBuffer): (Position, Size) = {
    val positionData = mappedByteBuffer.position()
    pageAlignedDataBuffer.buffer.flip()
    mappedByteBuffer.put(pageAlignedDataBuffer.buffer)
    mappedByteBuffer.force()
    (positionData, pageAlignedDataBuffer.buffer.capacity())
  }

  def writeIndex(mappedByteBuffer: MappedByteBuffer, indexByteBuffer: IndexByteBuffer): (Position, NoOfKeys) = {
    val indexPosition = mappedByteBuffer.position()
    indexByteBuffer.underlying.flip()
    mappedByteBuffer.put(indexByteBuffer.underlying)
    mappedByteBuffer.force()
    (indexPosition, indexByteBuffer.underlying.capacity() / config.indexKeySize)
  }

  def writeBloomFilter(mappedByteBuffer: MappedByteBuffer, bloomFilter: BitVector): (Position, Size) = {
    val bfilterPosition = mappedByteBuffer.position()
    val bfBuffer = bloomFilter.toByteBuffer
    mappedByteBuffer.put(bfBuffer)
    mappedByteBuffer.force()
    (bfilterPosition, bloomFilter.size.toInt)
  }

  def writeFooter(mappedByteBuffer: MappedByteBuffer, footer: Footer) = {
    mappedByteBuffer.position(Constants.SIXTY_FOUR_MB_BYTES - footer.noOfBytes)
    val ts = ByteVector.fromLong(footer.timeStamp.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val minKey = ByteVector.fromLong(footer.minKey.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val maxKey = ByteVector.fromLong(footer.maxKey.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val indexOffset = ByteVector.fromLong(footer.indexStartOffSet.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val noOfKeys = ByteVector.fromInt(footer.noOfKeysInIndex.underlying, Constants.INT_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val bfOffset = ByteVector.fromLong(footer.bfilterStartOffset.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val blockNo = ByteVector.fromInt(footer.blockNumber.underlying, Constants.INT_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val dataBufferOffset = ByteVector.fromLong(footer.dataBufferOffSet.underlying, Constants.LONG_SIZE_IN_BYTES, ByteOrdering.BigEndian)
    val dataBufferSize = ByteVector.fromInt(footer.dataBufferSize.underlying, Constants.INT_SIZE_IN_BYTES, ByteOrdering.BigEndian)

    mappedByteBuffer.put(ts.toByteBuffer)
    mappedByteBuffer.put(minKey.toByteBuffer)
    mappedByteBuffer.put(maxKey.toByteBuffer)
    mappedByteBuffer.put(dataBufferOffset.toByteBuffer)
    mappedByteBuffer.put(dataBufferSize.toByteBuffer)
    mappedByteBuffer.put(indexOffset.toByteBuffer)
    mappedByteBuffer.put(noOfKeys.toByteBuffer)
    mappedByteBuffer.put(bfOffset.toByteBuffer)
    mappedByteBuffer.put(blockNo.toByteBuffer)
    mappedByteBuffer.force()
    ()
  }


  def write[F[_]: Sync](blockWriterResult: BlockWriterResult) = {
    val fileF: FileF[F] = new FileF[F](config)
    def makeFile(file: RandomAccessFile): Resource[F, RandomAccessFile] = Resource.fromAutoCloseable(F.delay(file))
    def makeChannel(channel: FileChannel): Resource[F, FileChannel] = Resource.fromAutoCloseable(F.delay(channel))

    makeFile(fileF.rFile).use {
      file =>
        makeChannel(file.getChannel).use {
          channel =>
            val mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, Constants.SIXTY_FOUR_MB_BYTES)
            F.flatMap(C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(F.delay(writeData(mappedByteBuffer, blockWriterResult.underlying)))){
              case (dataPos, dataSize) =>
                F.flatMap(C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(F.delay(writeIndex(mappedByteBuffer, blockWriterResult.indexByteBuffer)))){
                  case (indexPos, noOfKeys) =>
                    F.flatMap(C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(F.delay(writeBloomFilter(mappedByteBuffer, blockWriterResult.bloomFilter)))){
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
                          writeFooter(mappedByteBuffer, footer)
                        }
                    }
                }
            }
        }
    }
  }

}
