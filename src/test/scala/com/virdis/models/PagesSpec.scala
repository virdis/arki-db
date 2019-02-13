
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

import cats.{Applicative, Traverse}
import cats.effect.{ContextShift, IO, Sync}
import com.virdis.{BaseSpec, commonF}
import com.virdis.utils.Utils
import cats.implicits._
import scala.concurrent.Future


class PagesSpec extends BaseSpec {
  implicit val cf : ContextShift[IO] = commonF.cf
  def cleanup(p: Pages) =
    Utils.pagesCleanUp(p)(Sync[IO], cf, implicitly[Traverse[List]], implicitly[Applicative[IO]]).unsafeRunSync()

  class Fixture {
   def pb = (0 until 10).toList.map {
      i =>
        val pb = PayloadBuffer(ByteBuffer.allocate(4).putInt(i))
        pb
    }
  }

  it should "calculate pageno" in {
    val p = new Pages(3, 32)
    Future { p.calculatePageNo(35) }.map {
      i =>
        cleanup(p)
        assert(i == 1)
    }
  }
  it should "add payload buffer, within page limit " in {
    val pg = new Pages(3, 32)
    val pb = PayloadBuffer(ByteBuffer.allocate(31))
    Future{ pg.add(pb) }.map {
      case(p,off) =>
        cleanup(pg)
        assert(p === Page(0) && off === Offset(0))
    }
  }

  it should "add payload buffer" in {
    val pg = new Pages(3, 16)
    val pbs = new Fixture().pb
    val list = pbs.map(p => pg.add(p))
    Future{ list.last }.map {
      case(p,off) =>
        cleanup(pg)
        assert(p === Page(2) && off === Offset(4))
    }
  }
  it should "getPosition" in {
    val pg = new Pages(3, 16)
    val pbs = new Fixture().pb
    val list = pbs.map(p => pg.add(p))
    Future { pg.getPosition }.map{
      i =>
        cleanup(pg)
        assert(i == 8)
    }
  }
  it should "getPageNo" in {
    val pg = new Pages(3, 16)
    val pbs = new Fixture().pb
    val list = pbs.map(p => pg.add(p))
    Future { pg.getPageNo }.map {
      i =>
        cleanup(pg)
        assert(i == 2)
    }
  }

}
