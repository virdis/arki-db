
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

package com.virdis.search.inmemory

import java.util
import cats.effect.{IO, Sync}
import com.virdis.BaseSpec
import com.virdis.models.{KeyByteVector, PayloadBuffer, ValueByteVector}
import com.virdis.utils.Constants
import scodec.bits.ByteVector
import cats.implicits._
import scala.collection.mutable.ListBuffer

class InMemoryMapSearchFSpec extends BaseSpec {
  implicit val syc          = Sync[IO]
  class Fixture {
    val immsf = new InMemoryMapSearchF[IO]()
    val map1  = new java.util.TreeMap[Long, PayloadBuffer]()
    ('a' to 'e').foreach {
      c =>
        val k = KeyByteVector(ByteVector.fromShort(c.toByte), Constants.BYTE_SIZE_IN_BYTES)
        val v = ValueByteVector(ByteVector.fromShort(c.toByte), Constants.BYTE_SIZE_IN_BYTES)
        map1.put(c.toLong, PayloadBuffer.fromKeyValue(k,v))
    }
    val map2  = new java.util.TreeMap[Long, PayloadBuffer]()
    ('f' to 'k').foreach{
      c =>
        val k = KeyByteVector(ByteVector.fromShort(c.toByte), Constants.BYTE_SIZE_IN_BYTES)
        val v = ValueByteVector(ByteVector.fromShort(c.toByte), Constants.BYTE_SIZE_IN_BYTES)
        map2.put(c.toLong, PayloadBuffer.fromKeyValue(k,v))
    }
    val listBufferMap = new ListBuffer[util.NavigableMap[Long, PayloadBuffer]]
    listBufferMap.prepend(map1)
    listBufferMap.prepend(map2)

    val check = (ch: Char) => immsf.searchListMap(ch.toLong, listBufferMap) == Option(PayloadBuffer.fromKeyValue(
      KeyByteVector(ByteVector.fromShort(ch.toByte), Constants.BYTE_SIZE_IN_BYTES),
      ValueByteVector(ByteVector.fromShort(ch.toByte), Constants.BYTE_SIZE_IN_BYTES)
    ))
  }
  it should "return Some(key) if present" in {
    val f = new Fixture
    import f._
    val res = ('a' to 'h').toList.map { c => check(c) }
    assert(res.forall(_ == true))
  }
  it should "return None if not present" in {
    val f = new Fixture
    import f._
    val res = ('l' to 'n').toList.map { c => check(c) }
    assert(res.forall(_ == false))
  }
  it should "put a NavigableMap in buffer" in {
    val f = new Fixture
    import f._
    val map3 = new util.TreeMap[Long, PayloadBuffer]()
    val io = immsf.putMapInBuffer(map1) *> immsf.putMapInBuffer(map2) *> immsf.putMapInBuffer(map3)
    val length: Int = (io *> immsf.internal.flatMap {
      ref =>
        ref.get.map(_.length)
    }).unsafeRunSync()
    assert(length == 3)
  }
  it should "search should return key if present" in {
    val f = new Fixture
    import f._
    val k = KeyByteVector(ByteVector.fromShort('a'.toByte), Constants.BYTE_SIZE_IN_BYTES)
    val v = ValueByteVector(ByteVector.fromShort('a'.toByte), Constants.BYTE_SIZE_IN_BYTES)
    val io = immsf.putMapInBuffer(map1) *> immsf.putMapInBuffer(map2)
    val res: Option[PayloadBuffer] = (io *> immsf.searchKey('a'.toLong)).unsafeRunSync()
    assert(res == Option(PayloadBuffer.fromKeyValue(k,v)))
  }
  it should "search should return None key is absent" in {
    val f = new Fixture
    import f._
    val k = KeyByteVector(ByteVector.fromShort('a'.toByte), Constants.BYTE_SIZE_IN_BYTES)
    val v = ValueByteVector(ByteVector.fromShort('a'.toByte), Constants.BYTE_SIZE_IN_BYTES)
    val io = immsf.putMapInBuffer(map1) *> immsf.putMapInBuffer(map2)
    val res: Option[PayloadBuffer] = (io *> immsf.searchKey('z'.toLong)).unsafeRunSync()
    assert(res == None)
  }
}
