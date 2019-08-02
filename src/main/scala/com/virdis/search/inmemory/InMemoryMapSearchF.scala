
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
import java.util

import cats.effect.Sync
import cats.effect.concurrent.Ref
import com.virdis.models.PayloadBuffer

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

final class InMemoryMapSearchF[F[_]](implicit F: Sync[F]) extends InMemoryMapSearch [F] {
  final val internal = Ref.of[F, mutable.ListBuffer[util.NavigableMap[Long, PayloadBuffer]]](ListBuffer.empty)

  final override def putMapInBuffer(map: util.NavigableMap[Long, PayloadBuffer]): F[Unit] = {
    F.flatMap(internal) {
      reflistBuff => reflistBuff.modify {
        listBuffer =>
          listBuffer.prepend(map)
          (listBuffer, F.unit)
      }
    }
  }

  final override def searchKey(key: Long): F[Option[PayloadBuffer]] = {
      F.flatMap(internal) {
        ref => {
          F.flatMap(ref.get) {
            listBuff => F.delay(searchListMap(key, listBuff))
          }
        }
      }
  }

  def searchListMap(key: Long,
                    listBufferMap: ListBuffer[util.NavigableMap[Long,PayloadBuffer]]
                   ) : Option[PayloadBuffer] = {
    listBufferMap.foldLeft(None: Option[PayloadBuffer]){
      (b, a) =>
        val found: Option[PayloadBuffer] = if(!b.isEmpty) b else Option(a.get(key))
        found
    }
  }

}
