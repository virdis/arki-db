
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

import com.virdis.BaseSpec

import scala.concurrent.Future

class PagesSpec extends BaseSpec {

  it should "calcualte pageno" in {
    val p = new Pages(3, 32)
    Future {p.calculatePageNo(35)}.map(i => assert(i === 1))
  }
  it should "add payload buffer" in {
    val p = new Pages(3, 32)
    val pb = PayloadBuffer(ByteBuffer.allocate(31))
    Future{p.add(pb)}.map
    { case(p,off) =>
      assert(p === Page(0) && off === Offset(0))
    }
  }
}
