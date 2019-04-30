
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
//TODO make it configuarble via external file
final class Config(
                        final val pageSize: Int          = Constants.PAGE_SIZE,
                        final val blockSize: Int         = Constants.SIXTY_FOUR_MB_BYTES,
                        final val footerSize: Int        = Constants.FOOTER_SIZE,
                        final val bloomFilterBits: Int   = Constants.BLOOM_FILTER_BITS,
                        final val bloomFilterHashes: Int = Constants.BLOOM_FILTER_HASHES,
                        final val dataDirectory: String  = Constants.HOME_DIRECTORY,
                        final val bfCacheSize: Long      = (Constants.BLOOM_FILTER_BITS * 10),
                        final val indexCacheSize: Long   = Constants.SIXTY_FOUR_MB_BYTES / 2,
                        final val dataCacheSize: Long    = Constants.SIXTY_FOUR_MB_BYTES / 2

                      ) {
  def indexKeySize: Int      = Constants.INDEX_KEY_SIZE
  def longSizeInBytes: Int   = Constants.LONG_SIZE_IN_BYTES
  def intSizeInBytes: Int    = Constants.INT_SIZE_IN_BYTES
  def shortSizeInBytes: Int  = Constants.SHORT_SIZE_IN_BYTES
  def byteSizeInBytes: Int   = Constants.BYTE_SIZE_IN_BYTES

  final val bloomSizeInBytes = bloomFilterBits / 8 // Bits -> Bytes
  final val isBloomEnabled: Boolean = bloomFilterBits != 0
  final val maxAllowedBlockSize: Int = blockSize - (bloomSizeInBytes + footerSize) // should be multiple of 2
  //TODO DOCUMENT WHEN TO USE
  // WHEN ADDING DATA TO IN MEMORY MAP
  final val pagesFromAllowBlockSize: Int = Math.floor(maxAllowedBlockSize / pageSize).toInt

  final val xxHashSeed = Constants.XXHASH_SEED

  def cacheSize(kind: CacheKind): Long = {
    kind match {
      case BFCache    => bfCacheSize
      case IndexCache => indexCacheSize
      case DataCache  => dataCacheSize
    }
  }

}

object Config {
  implicit val default = new Config()
}

