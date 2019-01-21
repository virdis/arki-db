
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

import cats.effect.IO
import com.virdis.search.BlockIndexSearch
import com.virdis.utils.Constants

import scala.concurrent.Future

class BlockIndexSearchSpec extends BaseSpec {

  class Fixture extends CommonFixtures {
    val bw = new BlockWriter[IO]() {}
    val bis = new BlockIndexSearch[IO]{}
  }
  it should "binarySearch IndexBuffer" in {
    val f = new Fixture
    import f._
    val map = addDataToMap
    val searchKeys = genListOfSearchKeys(100, map.size()).sample.get
    bw.build(map).unsafeToFuture().flatMap {
      bwriteResult =>
        val searchRes = searchKeys.map {
          k =>
            bis.binarySearch(bwriteResult.indexByteBuffer, k, Constants.INDEX_KEY_SIZE, 0, map.size()).unsafeToFuture()
        }
        Future.sequence(searchRes).map(r => assert(r.toSet == searchKeys.toSet))
    }
  }
}
