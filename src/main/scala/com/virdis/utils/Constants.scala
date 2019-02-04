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

package com.virdis.utils

import com.kenai.jffi.{MemoryIO, PageManager}
import Tags._
object Constants {


  final val memoryManager       = MemoryIO.getInstance()
  final val pageManager         = PageManager.getInstance()
  final val PAGE_SIZE           = pageManager.pageSize().toInt
  final val SIXTY_FOUR_MB_BYTES = 67108864 // 64 MB
  final val INDEX_KEY_SIZE      = 16 // KEY:PAGENO:OFFSET
  // TODO ADD DOCUMENTATION FOR THIS CHOICE
  final val BLOOM_FILTER_SIZE   = 1024 // 3MB
  final val FOOTER_SIZE         = 48 // TIMESTAMP:MIN:MAX:INDEXSTART:NUMBEROFKEYS:BFSTART:BLOCKNUMBER

  final val LONG_SIZE_IN_BYTES     = java.lang.Long.BYTES
  final val INT_SIZE_IN_BYTES      = java.lang.Integer.BYTES
  final val SHORT_SIZE_IN_BYTES    = java.lang.Short.BYTES
  final val BYTE_SIZE_IN_BYTES     = java.lang.Byte.BYTES

  object Test {
    final val TEST_PG_SIZE    = 64 // bytes
    final val TEST_BLCK_SIZE  = 512 // bytes
    final val TEST_BF_SIZE    = 16 // bytes
  }
}

final class Config[+T](
  final val pageSize: Int          = Constants.PAGE_SIZE,
  final val blockSize: Int         = Constants.SIXTY_FOUR_MB_BYTES,
  final val bloomFilterSize: Int   = Constants.BLOOM_FILTER_SIZE
                        ) {
  def indexKeySize: Int      = Constants.INDEX_KEY_SIZE
  def footerSize: Int        = Constants.FOOTER_SIZE
  def longSizeInBytes: Int   = Constants.LONG_SIZE_IN_BYTES
  def intSizeInBytes: Int    = Constants.INT_SIZE_IN_BYTES
  def shortSizeInBytes: Int  = Constants.SHORT_SIZE_IN_BYTES
  def byteSizeInBytes: Int   = Constants.BYTE_SIZE_IN_BYTES
}

object Config {

  implicit val default: Config[Default] = new Config[Default]()
  implicit val test: Config[Test] = new Config[Test](
    Constants.Test.TEST_PG_SIZE,
    Constants.Test.TEST_BLCK_SIZE,
    Constants.Test.TEST_BF_SIZE
  )
}