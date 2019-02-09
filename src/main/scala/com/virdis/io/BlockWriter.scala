
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
import com.virdis.models.Pages
import com.virdis.threadpools.IOThreadFactory
import com.virdis.utils.Config
import com.virdis.utils.Tags.MyConfig

class BlockWriter[F[_]](config: Config[MyConfig])(
  implicit F: Sync[F], C: ContextShift[F]
){

  private def build0(map: java.util.NavigableMap[Long, ByteBuffer])= {
    val keySet          = map.navigableKeySet()
    val iterator        = keySet.iterator()
    val indexBuffer     = ByteBuffer.allocateDirect(keySet.size() * config.indexKeySize)
    val dataBufferSize  = config.blockSize - (config.bloomFilterSize + config.footerSize + indexBuffer.capacity())
    val dataBuffer      = ByteBuffer.allocateDirect(dataBufferSize)
    val calculatedPages = Math.ceil(dataBufferSize.toDouble / config.pageSize).toInt
    val pages           = new Pages(calculatedPages, config.pageSize)
    while(iterator.hasNext) {
      val key: Long = iterator.next()
      val payload: ByteBuffer = map.get(key)
      payload.flip()
      val (page,offSet) = pages.add(payload)
      indexBuffer.putLong(key)
      indexBuffer.putInt(page.underlying)
      indexBuffer.putInt(offSet.underlying)
    }
    (pages, indexBuffer)
  }

  def build(map: java.util.NavigableMap[Long, ByteBuffer]) = {
    C.evalOn(IOThreadFactory.BLOCKING_IO_POOL.executionContext)(F.delay(build0(map)))
  }


  /////////////////////////////////////////////////////

  def merge()

}
