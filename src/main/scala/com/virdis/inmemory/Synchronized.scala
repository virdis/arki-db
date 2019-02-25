
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

package com.virdis.inmemory

import java.util
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{Concurrent, IO}
import cats.effect.concurrent.{Deferred, Ref, Semaphore}
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.collection.mutable
import scala.concurrent.ExecutionContext

sealed abstract class Synchronized[F[_], A] {

  /**
    * Runs the specified function on the resource `A`, or waits until
    * given exclusive access to the resource, and then runs the given
    * function. Can be cancelled while waiting on exclusive access.
    */
  def use[B](f: A => F[B]): F[B]
}

object Synchronized {
  def apply[F[_]](implicit F: Concurrent[F]): ApplyBuilders[F] =
    new ApplyBuilders(F)

  def of[F[_], A](a: A)(implicit F: Concurrent[F]): F[Synchronized[F, A]] =
    Deferred[F, Unit].flatMap { initial =>
      initial.complete(()).flatMap { _ =>
        Ref.of[F, Deferred[F, Unit]](initial).map { ref =>
          new Synchronized[F, A] {
            override def use[B](f: A => F[B]): F[B] =
              Deferred[F, Unit].flatMap { next =>
                F.bracket(ref.getAndSet(next)) { current =>
                  current.get.flatMap(_ => f(a))
                }(_ => next.complete(()))
              }
          }
        }
      }
    }

  final class ApplyBuilders[F[_]](private val F: Concurrent[F]) extends AnyVal {
    def of[A](a: A): F[Synchronized[F, A]] =
      Synchronized.of(a)(F)
  }

}
case class FrozenMap(map: util.NavigableMap[Int, Int]) extends AnyVal

class MapF {
  var map = new util.TreeMap[Int, Int]()
  val counter = new AtomicInteger(10)
  def testF(i: Int) = {
    var f = FrozenMap(new util.TreeMap[Int, Int]())
    (1 to i).foreach {
      i =>
        map.put(i,i)
        if (map.size() == 30) {
          f = FrozenMap(map)
          map = new util.TreeMap[Int, Int]()
        }
    }
    f
  }
  def countF = {
    if (counter.decrementAndGet() == 5) println("Reached ??? 5")
    println(s"Reached ${counter.get()}")
  }
}
object TestF {
  implicit val ctx = IO.contextShift(ExecutionContext.global)
  val ioc = Concurrent[IO]
  val s = Semaphore[IO](1)(implicitly[Concurrent[IO]])

  s.map {
    ss =>
      ss.acquire
      ss.release
  }

  def testF = {
    (1 to 20).foreach {
      i =>

    }
  }
}




object Ex {
  import cats.effect._
  import cats.effect.concurrent._
  import cats.implicits._
  import cats.effect.implicits._
  implicit val context = scala.concurrent.ExecutionContext.global
  implicit val cs = IO.contextShift(context)
  val _map = new scala.collection.mutable.TreeMap[Int, Deferred[IO, String]]()
  type Id = Int

  trait ReadThroughCache[F[_], A] {
    def get(id: Id): F[A]
  }

  object ReadThroughCache {
    def create[F[_]: Concurrent, A](
                                     retrieve: F[A],
                                     map : scala.collection.mutable.TreeMap[Id, Deferred[F, A]]
                                   ) : F[ReadThroughCache[F, A]] =
      Ref.of[F, scala.collection.mutable.Map[Id, Deferred[F, A]]](map).map { state =>
        new ReadThroughCache[F, A] {
          def get(id: Id): F[A] =
            Deferred[F, A].flatMap { newR =>
              def update = retrieve.flatMap(newR.complete).start *> newR.get

              state.modify { store =>
                store.get(id) match {
                  case Some(r) => store -> r.get
                  case None => (store += (id -> newR)) -> update
                }
              }.flatten
            }
        }
      }
  }
}









