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

import scodec.codecs.implicits._
import java.nio.ByteBuffer
import scodec.Codec
import scodec.bits.{BitVector, ByteVector}

object TestScodec {

  val b = ByteBuffer.allocate(4)
  b.putInt(1000)
  b.position(0)
  val bitsV = BitVector.view(b)

  val res = Codec.decode[Int](bitsV)


}