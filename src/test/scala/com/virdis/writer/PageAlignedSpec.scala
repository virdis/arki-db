
/*
 * MIT License
 *
 * Copyright (c) 2019 Sandeep Virdi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.virdis.writer

import java.nio.ByteBuffer

import cats.effect.{IO, Sync}
import com.virdis.utils.Utils
import org.scalacheck.Gen
import cats.implicits._

class PageAlignedSpec extends BaseSpec {
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
      Utils.freeDirectBuffer[IO](address)(Sync[IO])
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
