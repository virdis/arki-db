
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

package com.virdis.io

import cats.effect.IO
import com.virdis.BaseSpec
import com.virdis.utils.Config

class BlockWriterSpec extends BaseSpec {
  class Fixture {
    val config = new Config(
      pageSize = 1024,
      blockSize = 524288,
      bloomFilterSize = 64,
      footerSize = 48
    )
    val blockWriter = new BlockWriter[IO](config)
  }

  it should "" in {

    ???
  }
}


















