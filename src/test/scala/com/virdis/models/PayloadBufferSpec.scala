
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

import com.virdis.BaseSpec
import com.virdis.utils.Constants
import scodec.bits.{ByteOrdering, ByteVector}

class PayloadBufferSpec extends BaseSpec {
  it should "return key/value byte vectors" in {
    val b = ByteVector.fromShort('a', Constants.SHORT_SIZE_IN_BYTES, ordering = ByteOrdering.BigEndian)
    val kBV = KeyByteVector(b, b.size.toInt)
    val vBV = ValueByteVector(b, b.size.toInt)
    val payloadBuffer = PayloadBuffer.fromKeyValue(kBV, vBV)
    val (keyBV, valueBV) = PayloadBuffer.toKeyValueByteVector(payloadBuffer.underlying)
    assert(keyBV == kBV.underlying && valueBV == vBV.underlying)
  }
}
