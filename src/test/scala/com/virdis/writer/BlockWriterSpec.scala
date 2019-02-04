
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
import com.virdis.utils.Config
import com.virdis.utils.Config._
import com.virdis.utils.Tags.{Default, Test}

class BlockWriterSpec
  extends BaseSpec {


  class Fixture extends CommonFixtures {
    val config = implicitly[Config[Default]]
    val bw = new BlockWriter[IO](config) {}
  }
  // TODO CHANGE TEST :-)
  it should "build and return BlockWriterResult" in {
    val f = new Fixture
    import f._
    val map = smallData
    bw.build(map).flatMap {
      bwRes =>
        println(bwRes)
      IO(bwRes)
    }.unsafeToFuture().map {
      b => assert(b.getClass.getSimpleName.contains("BlockWriterResult"))
    }
  }

}
