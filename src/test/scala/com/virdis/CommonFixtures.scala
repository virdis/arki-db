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
import com.virdis.models._
import com.virdis.utils.Config
import org.scalacheck.Gen

trait CommonFixtures {
  implicit val cf: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)
  val testConfig = new Config()

 def genSearchKey(upperBound: Long) = {
   Gen.choose(1, upperBound)
 }
 def genListOfSearchKeys(noOfKeys: Int, upBound: Long) = {
   Gen.listOfN[Long](noOfKeys, genSearchKey(upBound))
 }
  def genFooter: Gen[Footer] = {
    for {
      gTs     <- Gen.chooseNum[Long](Long.MinValue, Long.MaxValue)
      gMax    <- Gen.chooseNum[Long](Long.MinValue, Long.MaxValue)
      gMin    <- Gen.chooseNum(Long.MinValue, Long.MaxValue).suchThat(_ < gMax)
      gIdxOff <- Gen.chooseNum(Long.MinValue, Long.MaxValue)
      gKysIdx <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
      gbfOff  <- Gen.chooseNum(Long.MinValue, Long.MaxValue)
      gblckNo <- Gen.choose(Int.MinValue, Int.MaxValue)
    } yield Footer(
      timeStamp = Ts(gTs), minKey = MinKey(gMin), maxKey = MaxKey(gMax), indexStartOffSet = IndexStartOffSet(gIdxOff),
      noOfKeysInIndex = NoOfKeysInIndex(gKysIdx), bfilterStartOffset = BFilterStartOffset(gbfOff), blockNumber = BlockNumber(gblckNo)
    )
  }

  def genBytes: Gen[Array[Byte]] = {
    val g = Gen.oneOf(
      Gen.alphaLowerStr,
      Gen.alphaNumChar,
      Gen.alphaStr,
      Gen.uuid,
      Gen.numStr
    )
    g.map(_.toString.getBytes)
  }

  def listOfGenBytes(n:Int): List[Array[Byte]] =
    Gen.listOfN(n, genBytes).sample.get



}

object commonF extends CommonFixtures