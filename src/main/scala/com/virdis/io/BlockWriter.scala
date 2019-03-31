
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

import cats.effect.{ContextShift, Sync}
import com.virdis.bloom.BloomFilter
import com.virdis.models._
import com.virdis.threadpools.IOThreadFactory
import com.virdis.utils.{Config, Utils}
import scodec.bits.BitVector

final class BlockWriter[F[_]](config: Config)(
  implicit F: Sync[F], C: ContextShift[F]
){

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

}
