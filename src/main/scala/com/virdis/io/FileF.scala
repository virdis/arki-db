
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

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel

import cats.effect.{ContextShift, Resource, Sync}
import com.virdis.utils.Config

final class FileF[F[_]](config: Config)(implicit F: Sync[F], C: ContextShift[F]) {

  def name: F[String] = {
    F.flatMap(F.delay(System.nanoTime()))(ns => F.point(s"${ns}_lvl1.bin"))
  }

  def file: F[File] = {
    for {
      n <- name
    } yield new File(config.dataDirectory, n)
  }

  def rFile: RandomAccessFile = ???


}
