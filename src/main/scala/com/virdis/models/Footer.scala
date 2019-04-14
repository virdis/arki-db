
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

package com.virdis.models

import com.virdis.utils.Constants

case class Ts(underlying: Long)                 extends AnyVal
case class MinKey(underlying: Long)             extends AnyVal
case class MaxKey(underlying: Long)             extends AnyVal
case class IndexStartOffSet(underlying: Long)   extends AnyVal
case class NoOfKeysInIndex(underlying: Int)     extends AnyVal
case class BFilterStartOffset(underlying: Long) extends AnyVal
case class BlockNumber(underlying: Int)         extends AnyVal
case class DataBufferOffSet(underlying: Long)   extends AnyVal
case class DataBufferSize(underlying: Int)      extends AnyVal
case class Footer(
                 timeStamp: Ts,
                 minKey: MinKey,
                 maxKey: MaxKey,
                 dataBufferOffSet: DataBufferOffSet,
                 dataBufferSize: DataBufferSize,
                 indexStartOffSet: IndexStartOffSet,
                 noOfKeysInIndex: NoOfKeysInIndex,
                 bfilterStartOffset: BFilterStartOffset,
                 blockNumber: BlockNumber
                 ) {
  val noOfBytes: Int = {
      Constants.LONG_SIZE_IN_BYTES // Ts
    + Constants.LONG_SIZE_IN_BYTES // MinKey
    + Constants.LONG_SIZE_IN_BYTES // MaxKey
    + Constants.LONG_SIZE_IN_BYTES // IndexOffSet
    + Constants.INT_SIZE_IN_BYTES  // NoOfKeys
    + Constants.LONG_SIZE_IN_BYTES // BloomFilterOffSet
    + Constants.INT_SIZE_IN_BYTES  // BlockNumber
    + Constants.LONG_SIZE_IN_BYTES // DataBufferOffSet
    + Constants.INT_SIZE_IN_BYTES  // DataBufferSize
  }
}