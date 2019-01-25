
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

package com.virdis.writer

import java.nio.ByteBuffer

import cats.effect.{ContextShift, IO, Sync}
import com.virdis.utils.Utils
import org.scalacheck.Gen
import cats.implicits._

import scala.concurrent.ExecutionContext

class PageAlignedSpec extends BaseSpec {
  implicit val cf = IO.contextShift(scala.concurrent.ExecutionContext.global)
  class Fixture {
    val blockWriter = new BlockWriter[IO]() {}

    def genLong =
      for {
        gLong <- Gen.choose(Long.MinValue, Long.MaxValue)
      } yield gLong

    def populateByteBuffer(buff: ByteBuffer): Gen[ByteBuffer] = {
      for {
        long1 <- genLong
        _ = buff.putLong(long1)
        long2 <- genLong
        _ = buff.putLong(long2)
        _ = buff.flip()
      } yield buff
    }

    def cleanUp(address: Long) =
      Utils.freeDirectBuffer[IO](address)(Sync[IO], cf)
  }

  it should "put data" in {
    val f = new Fixture
    import f._
    val pageAligned = new blockWriter.PageAligned()
    val buffer = ByteBuffer.allocate(8 * 2)
    val resultPos = for {
      _       <- pageAligned.put(buffer)
      pos     <- pageAligned.position
      address <- pageAligned.nativeAddress
      _ = cleanUp(address)
    } yield pos

    resultPos.unsafeToFuture().map {
      p =>  assert(p == 16)
    }

  }
}
