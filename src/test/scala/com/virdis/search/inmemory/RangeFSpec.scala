
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

import cats.effect.{Concurrent, IO, Sync}
import com.virdis.BaseSpec
import com.virdis.models.{BFilterStartOffset, BlockNumber, DataBufferOffSet, DataBufferSize, Footer, IndexStartOffSet, MaxKey, MinKey, NoOfKeysInIndex, RangeFValue, Ts}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import cats.implicits._

import scala.concurrent.Future

class RangeFSpec extends BaseSpec {
  implicit val concurrentIO = Concurrent[IO]
  implicit val syc = Sync[IO]
  val random = new Random()
  class Fixture {
    val rangeF = new RangeF[IO]()

    def buildFooter = {
      val max = Math.abs(random.nextLong())
      Footer(
        Ts(System.nanoTime()),
        MinKey(max - 1000),
        MaxKey(max),
        DataBufferOffSet(random.nextLong()),
        DataBufferSize(random.nextInt()),
        IndexStartOffSet(random.nextLong()),
        NoOfKeysInIndex(random.nextInt()),
        BFilterStartOffset(random.nextLong()),
        BlockNumber(1)
      )
    }
    def buildRange = {
      val max = Math.abs(random.nextLong())
      val footer = Footer(
        Ts(System.nanoTime()),
        MinKey(max - 1000),
        MaxKey(max),
        DataBufferOffSet(random.nextLong()),
        DataBufferSize(random.nextInt()),
        IndexStartOffSet(random.nextLong()),
        NoOfKeysInIndex(random.nextInt()),
        BFilterStartOffset(random.nextLong()),
        BlockNumber(1)
      )
      (footer, max - 500)
    }
  }

  it should "add" in {
    val f = new Fixture
    import f._
    val buffer = mutable.ListBuffer.empty[Footer]
    (1 to 1000).toList.map {
      i =>
        val footer = buildFooter
        buffer.append(footer)
        rangeF.add(RangeFValue("a", footer)).start.unsafeRunSync()
    }
    val res = buffer.toList.map {
      f => rangeF.get(f.minKey.underlying)
    }
    assert(res.sequence.unsafeRunSync().map(_.get.footer).toSet == buffer.toSet)
  }
  it should "search range" in {
    val f = new Fixture
    import f._
    val buffer = mutable.ListBuffer.empty[Footer]
    val searchKeyBuffer = mutable.ListBuffer.empty[Long]
    (1 to 1000).toList.map {
      i =>
        val (footer, searchKey) = buildRange
        buffer.append(footer)
        searchKeyBuffer.append(searchKey)
        rangeF.add(RangeFValue("b", footer)).start.unsafeRunSync()
    }
    val res = searchKeyBuffer.toList.map {
      k => rangeF.get(k)
    }
    assert(res.sequence.unsafeRunSync().map(_.get.footer).toSet == buffer.toSet)

  }

}
