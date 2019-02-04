
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

import cats.effect.{IO, Sync}
import com.virdis.models.{BlockWriterResult, SearchResult}
import com.virdis.search.BlockIndexSearch
import com.virdis.utils.Tags.{Default, Test}
import com.virdis.utils.{Config, Constants, Utils}

import scala.concurrent.Future

class BlockIndexSearchSpec extends BaseSpec {

  class Fixture extends CommonFixtures {
    val config = implicitly[Config[Default]]
    val bw  = new BlockWriter[IO](config) {}
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
        Future.sequence(searchRes).map(r => assert(r.map(_.key.underlying).toSet === searchKeys.toSet))
    }
  }
  it should "help find data in DataBuffer" in {
    val f = new Fixture
    import f._
    val map = smallData
    val searchKeys = genListOfSearchKeys(10, map.size()).sample.get
    bw.build(map).unsafeToFuture().flatMap {
      bwriteResult =>
        val searchRes = searchKeys.map {
          k =>
            bis.binarySearch(bwriteResult.indexByteBuffer, k, Constants.INDEX_KEY_SIZE, 0, map.size()).unsafeToFuture()
        }
        Future.sequence(searchRes).map {
          list =>
            list.map {
              searchRes =>
                val SearchResult(key, page, offset) = searchRes
                val calculatedAddress = Utils.calculateOffset0(page.underlying, offset.underlying, config.pageSize)
                val (a1,a2) = Utils.kvByteBuffers[IO](calculatedAddress, bwriteResult.dataByteBuffer)(Sync[IO], cf)
                val _key = ByteBuffer.wrap(a1).getInt // Hard coded , we know its an INT_key === key
                _key === key.underlying
            }.foldLeft(true)(_ && _)
        }.map(r => assert(r))
    }
  }
}
