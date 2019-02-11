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

import java.nio.ByteBuffer
import java.util

import cats.effect.{ContextShift, IO}
import com.virdis.models._
import com.virdis.utils.Config
import com.virdis.utils.Tags.Test
import org.scalacheck.Gen

trait CommonFixtures {
  implicit val cf: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)
  val testConfig = implicitly[Config[Test]]

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

  def variableKeyValue = {
    var totalSize = 0
    val allowedSize = testConfig.blockSize - (testConfig.bloomFilterSize + testConfig.footerSize) // 512 - (16+48)

    def go(acc: List[Array[Byte]], total: Int): List[Array[Byte]] = {
      val arrBytes = listOfGenBytes(1)
      val arrSize = arrBytes.headOption.map(_.size).getOrElse(0)
      val currentTotal = total + arrSize
      if (currentTotal < allowedSize ) {
        if (arrSize < testConfig.pageSize ) go(acc ++ arrBytes, currentTotal) else  go(acc, total)
      } else acc
    }

    val data = go(List.empty[Array[Byte]], totalSize)
    println(s"ARRAY SIZE=${data.size}")
    val (map, _, _, accPayloadByteBuffer) = data.foldLeft(
      (new util.TreeMap[Long, ByteBuffer], 0, 0, List.empty[ByteBuffer])
    ){
      case ((acc, keyCounter, sizeCounter, accPayloadBuffer), a) =>
        val generatedKey: Long = keyCounter
        val keyBuffer   = ByteBuffer.wrap(a)
        val valueBuffer = ByteBuffer.wrap(a)
        val payloadByteBuffer = ByteBuffer.allocate((2 * a.size) + 5)
        payloadByteBuffer.putShort(keyBuffer.capacity().toShort)
        payloadByteBuffer.put(keyBuffer)
        payloadByteBuffer.putShort(valueBuffer.capacity().toShort)
        payloadByteBuffer.put(valueBuffer)
        payloadByteBuffer.put(1.toByte)
        if(payloadByteBuffer.capacity() + 8 + sizeCounter < allowedSize) {
          acc.put(generatedKey, payloadByteBuffer)
          accPayloadBuffer :+ payloadByteBuffer.duplicate()
          (acc, keyCounter + 1, sizeCounter + 8 + payloadByteBuffer.capacity(), accPayloadBuffer)
        } else {
          (acc, keyCounter, sizeCounter, accPayloadBuffer)
        }

    }
    (map, accPayloadByteBuffer)

  }

}

object commonF extends CommonFixtures