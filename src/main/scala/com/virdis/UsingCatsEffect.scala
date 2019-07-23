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

import java.util.concurrent.Executors

import cats.effect.{Async, IO}
import cats.implicits._

import scala.concurrent.ExecutionContext

object UsingCatsEffect extends App {

  val ec1 = ExecutionContext.fromExecutor(Executors.newCachedThreadPool(new NamedThreadFactory("ec1", true)))
  val ec2 = ExecutionContext.fromExecutor(Executors.newCachedThreadPool(new NamedThreadFactory("ec2", true)))
  val ec3 = Executors.newCachedThreadPool(new NamedThreadFactory("ec3", true))

  val cs1 = IO.contextShift(ec1)
  val cs2 = IO.contextShift(ec2)

  val printThread = IO { println(Thread.currentThread().getName) }

  val a = IO.async[Unit] { cb =>
    ec3.submit(new Runnable {
      override def run(): Unit = {
        println(Thread.currentThread().getName + " (async)")
        cb(Right(()))
      }
    })
  }

  def run(name: String)(th: IO[_]): Unit = {
    println(s"-- $name --")
    try {
      th.unsafeRunSync()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    println()
  }

  run("Plain") {
    printThread
  }

  run("Shift") {
    printThread *> IO.shift(ec1) *> printThread *> IO.shift(ec2) *> printThread
  }

  run("Shift shift") {
    IO.shift(ec1) *> IO.shift(ec2) *> printThread
  }

  run("Eval on") {
    printThread *> cs1.evalOn(ec2)(printThread) *> printThread
  }

  run("Eval on eval on") {
    cs2.evalOn(ec2)(cs1.evalOn(ec1)(printThread))
  }

  run("caveat 1") {
    val someEffect = IO.shift(ec1) *> printThread
    printThread *> someEffect *> printThread
  }

  run("async") {
    printThread *> a *> printThread
  }

  run("async shift") {
    a *> IO.shift(ec1) *> printThread
  }

  run("async 2") {
    cs1.evalOn(ec1)(a *> printThread)
  }

  val ae = IO.async[Unit] { cb =>
    ec3.submit(new Runnable {
      override def run(): Unit = {
        println(Thread.currentThread().getName + " (async)")
        cb(Left(new IllegalStateException()))
      }
    })
  }

  run("async error") {
    ae.guarantee(printThread)
  }

  run("async shift error") {
    ae.guarantee(IO.shift(ec1)).guarantee(printThread)
  }

  val nf = ae.guarantee(Async.shift(ec1)(implicitly[Async[IO]])).guarantee(printThread)

  run("ae gurantee Async") {
    nf
  }
}
