
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
import com.virdis.models._
import com.virdis.threadpools.IOThreadFactory
import com.virdis.threadpools.ThreadPool.BlockingIOPool
import com.virdis.utils.{Config, Utils}
import com.virdis.utils.Tags.MyConfig

class BlockWriter[F[_]](config: Config[MyConfig])(
  implicit F: Sync[F], C: ContextShift[F]
){

  private def build0(map: java.util.NavigableMap[Long, PayloadBuffer])= {
    val keySet          = map.navigableKeySet()
    val iterator        = keySet.iterator()
    val indexBuffer     = new IndexByteBuffer(ByteBuffer.allocateDirect(keySet.size() * config.indexKeySize))
    val dataBufferSize  = config.blockSize - (config.bloomFilterSize + config.footerSize + indexBuffer.underlying.capacity())
    val dataBuffer      = ByteBuffer.allocateDirect(dataBufferSize)
    val calculatedPages = Math.floor(dataBufferSize.toDouble / config.pageSize).toInt
    val pages           = new Pages(calculatedPages, config.pageSize)
    while(iterator.hasNext) {
      val key: Long = iterator.next()
      val pb: PayloadBuffer = map.get(key)
      pb.payload.flip()
      // we dont need bound check here since the map will be under maxAllowedBlockSize
      val (page,offSet) = pages.add(pb)
      indexBuffer.add(Key(key), page, offSet)
    }
    BlockWriterResult(pages, indexBuffer)
  }

  def build(map: java.util.NavigableMap[Long, PayloadBuffer])() = {
    C.evalOn(IOThreadFactory.blockingIOPool.executionContext)(F.delay(build0(map)))
  }


  /////////////////////////////////////////////////////

  def merge0(block1: Block, block2: Block) = {
    val idx1 = block1.index
    val idx2 = block2.index
    val db1  = block1.data
    val db2  = block2.data
    val mg   = new MergeBlockResult(config)
    while(
      idx1.underlying.hasRemaining
      && idx2.underlying.hasRemaining
      && idx1.checkBounds
      && idx2.checkBounds
    ){

      val (key1, page1, offset1) = idx1.getIndexElement
      val (key2, page2, offset2) = idx2.getIndexElement
      val position1: Int         = Utils.calculateOffset(page1, offset1, config.pageSize)
      val position2: Int         = Utils.calculateOffset(page2, offset2, config.pageSize)
      val pb1: PayloadBuffer     = db1.getDataBufferElement(position1)
      val pb2: PayloadBuffer     = db2.getDataBufferElement(position2)
      if (key1.underlying < key2.underlying) {
        mg.add(key1, pb1)
        mg.add(key2, pb2)
      } else if (key1.underlying > key2.underlying) {
        mg.add(key2, pb2)
        mg.add(key1, pb1)
      } else if (block1.footer.timeStamp.underlying < block2.footer.timeStamp.underlying){ // check timestamp
        mg.add(key1, pb1)
      } else {
        mg.add(key2, pb2)
      }
    }
    while(idx1.underlying.hasRemaining && idx1.checkBounds) {
      val (key1, page1, offset1) = idx1.getIndexElement
      val position1: Int         = Utils.calculateOffset(page1, offset1, config.pageSize)
      val pb1: PayloadBuffer     = db1.getDataBufferElement(position1)
      mg.add(key1, pb1)
    }
    while(idx2.underlying.hasRemaining && idx2.checkBounds){
      val (key2, page2, offset2) = idx2.getIndexElement
      val position2: Int         = Utils.calculateOffset(page2, offset2, config.pageSize)
      val pb2: PayloadBuffer     = db2.getDataBufferElement(position2)
      mg.add(key2, pb2)
    }
    mg
  }

  def merge(b1: Block, b2: Block) =
    C.evalOn(IOThreadFactory.blockingIOPool.executionContext){ F.delay(merge0(b1, b2)) }
}
