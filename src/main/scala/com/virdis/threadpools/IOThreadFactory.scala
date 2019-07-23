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

package com.virdis.threadpools

import java.util.concurrent.{Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

import cats.effect
import cats.effect.syntax.effect
import cats.effect.{ContextShift, IO}
import com.virdis.threadpools.ThreadPool._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

trait IOThreadFactory[A] {
  def executionContext: ExecutionContextExecutor
}

object IOThreadFactory {

  implicit final val blockingIOPool: IOThreadFactory[BlockingIOPool] = new IOThreadFactory[BlockingIOPool] {

    final class BlockingIOThreadFactory() extends ThreadFactory {
      private final val poolNumber = new AtomicInteger(1)
      private final val threadNo = new AtomicInteger(1)
      private final val group: ThreadGroup = Thread.currentThread().getThreadGroup()
      private final val prefix = "blocking-io-" + poolNumber.incrementAndGet() + "-thread"

      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(group, runnable,
          prefix + threadNo.getAndIncrement(), 0)
        thread
      }
    }

    override final val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(
      Executors.newCachedThreadPool(
        new BlockingIOThreadFactory()
      )
    )
  }

  implicit final val nonBlockingPool: IOThreadFactory[GlobalPool] = new IOThreadFactory[GlobalPool] {

    override final val executionContext: ExecutionContextExecutor = ExecutionContext.global

  }
}
