
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

package com.virdis.search.inmemory

import cats.effect.{Concurrent, ContextShift, IO, Sync}
import cats.effect.concurrent.{MVar, Ref}
import com.virdis.models.{Footer, RangeFValue}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import cats.collections.{Range => CatsRange}
// TODO replace this with Interval Tree
final class RangeF[F[_]]()(implicit F:  Sync[F], C: Concurrent[F]) {

  // order should be recent to old
  private final val refListBuffer = Ref.of[F, mutable.ListBuffer[RangeFValue]](ListBuffer.empty)

  def add(rangev: RangeFValue): F[Unit] = {
    F.flatMap(refListBuffer) {
      rlb =>
       rlb.modify {
         lb =>
           lb.prepend(rangev)
           (lb, F.unit)
       }
    }
  }

  def get(key: Long): F[Option[RangeFValue]] = {
    F.flatMap(refListBuffer) {
      rlb =>
        F.flatMap(rlb.get){
          lb =>
            F.delay(lb.find(_.range.contains(key)(cats.instances.long.catsKernelStdOrderForLong)))
        }
    }
  }
}
