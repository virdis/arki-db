
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

import cats.effect.{ContextShift, Sync}
import com.virdis.utils.Utils

case class Block(
                data:  ByteBuffer,
                index: ByteBuffer,
                bloomFilter: ByteBuffer,
                footer: Footer
                ) {

  def clean[F[_]]()(implicit F: Sync[F],
                    Cs: ContextShift[F]) = {
    Utils.freeDirectBuffer[F](data)(F, Cs)
    Utils.freeDirectBuffer[F](index)(F, Cs)
    Utils.freeDirectBuffer[F](bloomFilter)(F, Cs)
  }

}