
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

// TODO replace this with Interval Tree
final class RangeF[F[_]]()(implicit F:  Sync[F], C: Concurrent[F]) {
  final val ordering = new Ordering[Long] {
    override def compare(x: Long, y: Long): Int = x compareTo y
  }
  private val fMVarMap = MVar.of[F, mutable.TreeMap[Long, RangeFValue]](
    new mutable.TreeMap[Long, RangeFValue]()(ordering.reverse))

  def add(rangev: RangeFValue): F[Option[RangeFValue]] = {
    F.flatMap(fMVarMap) {
      mvarMap =>
        F.flatMap(mvarMap.take) { // semantic blocking
          map =>
            F.delay(map.put(rangev.footer.minKey.underlying, rangev))
        }
    }
  }

  def get(key: Long): F[Option[RangeFValue]] = {
    F.flatMap(fMVarMap) {
      mvarMap =>
        F.flatMap(mvarMap.read) {
          map =>
            F.delay(map.get(key) orElse search(key, map))
        }
    }
  }

  def search(key: Long, map: mutable.TreeMap[Long, RangeFValue]): Option[RangeFValue] = {
    def go(iterator: Iterator[Long]): Option[RangeFValue] = {
      if(iterator.hasNext) {
        val mMinKey = iterator.next()
        val mRangeFValue = map(mMinKey)
        if (mMinKey < key && key < mRangeFValue.footer.maxKey.underlying) Option(mRangeFValue)
        else go(iterator)
      } else {
        None
      }
    }
    go(map.keysIterator)
  }

}
