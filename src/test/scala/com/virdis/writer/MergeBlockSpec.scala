
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
import com.virdis.models.{Block, Index}
import com.virdis.utils.Config
import org.scalacheck.Gen
import com.virdis.utils.Config._
import com.virdis.utils.Tags.Test

class MergeBlockSpec extends BaseSpec {

  class Fixture extends CommonFixtures {
    val config = implicitly[Config[Test]]
    val mg = new MergeBlock[IO](config){}
    val bw = new BlockWriter[IO](config) {}
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
    val indexes = listOfBytes.foldLeft(List.empty[Int]) {
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
  it should "merge blocks" in {
    val f = new Fixture
    import f._
    val oddMap = CommonFixtures.testConfigMap(false)
    println(s"ODD MAP=${oddMap}")
    val oddBlockWRes = bw.build(oddMap)
    val evenMap = CommonFixtures.testConfigMap(true)
    println(s"EVEN MAP=${evenMap}")
    val evenBlockWRes = bw.build(evenMap)
    val footer1 = genFooter
    val footer2 = genFooter
    oddBlockWRes.flatMap {
      oddBlockWR =>
        evenBlockWRes.flatMap {
          evenBlockWR =>
            val oddBlock = Block(
              oddBlockWR.dataByteBuffer,
              oddBlockWR.indexByteBuffer,
              ByteBuffer.allocate(0),
              footer1.sample.get
            )

            val evenBlock = Block(
              evenBlockWR.dataByteBuffer,
              evenBlockWR.indexByteBuffer,
              ByteBuffer.allocate(0),
              footer2.sample.get
            )
            val mergeBlockResult = mg.merge(oddBlock, evenBlock)
            val map1 = mergeBlockResult.map1
            val map2 = mergeBlockResult.map2
            val keySetIter1 = map1.entrySet().iterator()
            val keySetIter2 = map2.entrySet().iterator()
            var res = true
            while (keySetIter1.hasNext && keySetIter2.hasNext) {
              val key1 = keySetIter1.next().getKey
              val key2 = keySetIter2.next().getKey
              res = res && (key1 < key2)
            }
            IO(res)
        }
    }.unsafeToFuture().map{ b => assert(b) }

  }
}
