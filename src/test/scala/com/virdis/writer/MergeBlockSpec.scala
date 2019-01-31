
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

import cats.effect.IO
import com.sun.xml.internal.ws.encoding.MtomCodec.ByteArrayBuffer
import com.virdis.io.MergeBlock
import com.virdis.models.Index
import org.scalacheck.Gen

class MergeBlockSpec extends BaseSpec {

  class Fixture extends CommonFixtures {
    val mg = new MergeBlock[IO](){}
    def genBytes = {
      val g = Gen.oneOf(
        Gen.alphaLowerStr,
        Gen.alphaNumChar,
        Gen.alphaStr,
        Gen.uuid,
        Gen.numStr
      )
      g.map(_.toString.getBytes)
    }
    def listOfGenBytes(n:Int) =
      Gen.listOfN(n, genBytes).sample.get
  }

  it should "return payloadByteBuffers" in {
    val f = new Fixture
    import f._
    val listOfBytes = listOfGenBytes(100)
    val totalSize = listOfBytes.foldLeft(0)((acc, a) => acc + a.size)
    val bytebuffer = ByteBuffer.allocate((totalSize * 2) + (4 * listOfBytes.size))
    val indexes = listOfBytes.foldLeft(List.empty[Int]){
      (idx, a) =>
        val accIdx = idx :+ bytebuffer.position()
        bytebuffer.putShort(a.size.toShort) // key size
        bytebuffer.put(a) // key
        bytebuffer.putShort(a.size.toShort) // value size
        bytebuffer.put(a) // value

        accIdx
    }
    bytebuffer.flip()
    val pbs = indexes.map {
      ix =>

        mg.payloadByteBuffers(Index(ix), ix, bytebuffer)
    }
    val resultString = pbs.map(pb => new String(pb.payload.array()))
    val listOfString = listOfBytes.map(s => new String(s))

    assert(resultString.zip(listOfString).forall { case (a, b) => a.contains(b) })
  }
}