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

  object App1 {
    trait Program[F[_]] {
      def finish[A](a: => A): F[A]

      def chain[A, B](fa: F[A], afb: A => F[B]): F[B]

      def map[A, B](fa: F[A], ab: A => B): F[B]
    }
    object Program {
      def apply[F[_]](implicit F: Program[F]): Program[F] = F
    }
    implicit class ProgramSyntax[F[_], A](fa: F[A]) {
      def map[B](f: A => B)(implicit F: Program[F]): F[B] = F.map(fa, f)
      def flatMap[B](afb: A => F[B])(implicit F: Program[F]): F[B] = F.chain(fa, afb)
    }
    trait Console[F[_]] {
      def putStrLn(line: String): F[Unit]
      def getStrLn: F[String]
    }
    object Console {
      def apply[F[_]](implicit F: Console[F]): Console[F] = F
    }
    def putStrLn[F[_]: Console](line: String): F[Unit] = Console[F].putStrLn(line)
    def getStrLn[F[_]: Console]: F[String] = Console[F].getStrLn
    def finish[F[_], A](a: => A)(implicit F: Program[F]): F[A] = F.finish(a)

    object Test {
      def checkContinue[F[_]: Program: Console](name: String): F[Boolean] =
        for {
          _     <- putStrLn("Do you want to continue, " + name + "?")
          input <- getStrLn.map(_.toLowerCase)
          cont  <- input match {
            case "y" => finish(true)
            case "n" => finish(false)
            case _   => checkContinue(name)
          }
        } yield cont
    }
  }
}
