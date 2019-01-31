
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

import java.nio.ByteBuffer
import java.util

class MergeBlockResult {
  val map1: java.util.TreeMap[Long, ByteBuffer] = new util.TreeMap()
  val map2: java.util.TreeMap[Long, ByteBuffer] = new util.TreeMap()

  def add(payloadBuffer: PayloadBuffer, flag: Boolean) = {
    if (flag) map1.put(payloadBuffer.key, payloadBuffer.payload)
    else map2.put(payloadBuffer.key, payloadBuffer.payload)

  }

}