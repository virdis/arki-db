
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

import com.virdis.utils.Tags.{Default, Test}

final class Config[+T](
                        final val pageSize: Int          = Constants.PAGE_SIZE,
                        final val blockSize: Int         = Constants.SIXTY_FOUR_MB_BYTES,
                        final val bloomFilterSize: Int   = Constants.BLOOM_FILTER_SIZE,
                        final val footerSize: Int        = Constants.FOOTER_SIZE
                      ) {
  def indexKeySize: Int      = Constants.INDEX_KEY_SIZE
  def longSizeInBytes: Int   = Constants.LONG_SIZE_IN_BYTES
  def intSizeInBytes: Int    = Constants.INT_SIZE_IN_BYTES
  def shortSizeInBytes: Int  = Constants.SHORT_SIZE_IN_BYTES
  def byteSizeInBytes: Int   = Constants.BYTE_SIZE_IN_BYTES

  final val maxAllowedBlockSize: Int = blockSize - (bloomFilterSize + footerSize)
  //TODO DOCUMENT WHEN TO USE
  // WHEN ADDING DATA TO IN MEMORY MAP
  final val pagesFromAllowBlockSize: Int = Math.floor(maxAllowedBlockSize / pageSize).toInt

}

object Config {

  implicit val default: Config[Default] = new Config[Default]()
  implicit val test: Config[Test] = new Config[Test](
    Constants.Test.TEST_PG_SIZE,
    Constants.Test.TEST_BLCK_SIZE,
    Constants.Test.TEST_BF_SIZE,
    Constants.Test.TEST_FOOTER_SIZE
  )
}

