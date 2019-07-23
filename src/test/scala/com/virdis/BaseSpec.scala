

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

package com.virdis

import cats.effect.{ContextShift, IO}
import com.virdis.threadpools.IOThreadFactory
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class BaseSpec extends AsyncFlatSpec with Matchers {
  implicit val ioShift: ContextShift[IO] = IO.contextShift(IOThreadFactory.blockingIOPool.executionContext)
  implicit val globalPool: ExecutionContext = IOThreadFactory.nonBlockingPool.executionContext
}
