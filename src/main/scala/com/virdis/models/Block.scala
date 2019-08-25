
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

import cats.effect.{ContextShift, Sync}
import com.virdis.utils.Utils

/**
  *                      Block Design:Default size 64 MB
  *              |--------------------------------------------------|
  *              | DataByteBuffer - stores payload in               |
  *              |                  page aligned data block.        |
  *              |                  Default size of data block      |
  *              |                  4096 bytes.                     |
  *              |--------------------------------------------------|
  *              | Index          - stores key, page and offset.    |
  *              |                  page represents the numerical   |
  *              |                  value of the page aligned data  |
  *              |                  block, offset represents the    |
  *              |                  index of the data in the page.  |
  *              |--------------------------------------------------|
  *              | BloomFilter    - use to check if the key is      |
  *              |                  present in the block.           |
  *              |--------------------------------------------------|
  *              | Footer                                           |
  *              |                                                  |
  *              |  ----------------------------------------------  |
  *              | | a |  b |  d  |  e |  f |  g  |  h |  i |  j  | |
  *              |  ----------------------------------------------  |
  *              |  a - Timestamp (creation time)                   |
  *              |  b - MinKey (smallest key in the block)          |
  *              |  c - MaxKey (largest key in the block)           |
  *              |  d - DataBufferOffset (offset in the block where |
  *              |      DataBuffer is written)                      |
  *              |  e - DataBufferSize (size of DataBuffer in bytes)|
  *              |  f - IndexStartOffset (offset in the block where |
  *              |      Index is written)                           |
  *              |  g - NoOfKeysInIndex (number of keys in index)   |
  *              |  h - BloomFilterOffset (offset in the block where|
  *              |      bloomfilter is written)                     |
  *              |  i - BlockNumber (block number in the file)      |
  *               --------------------------------------------------
  *
  */

final case class Block(
                data:  DataByteBuffer,
                index: IndexByteBuffer,
                bloomFilter: ByteBuffer,
                footer: Footer
                ) {

  def clean[F[_]]()(implicit F: Sync[F],
                    Cs: ContextShift[F]): F[Unit] = {
    Utils.freeDirectBuffer[F](data.underlying)(F, Cs)
    Utils.freeDirectBuffer[F](index.underlying)(F, Cs)
    Utils.freeDirectBuffer[F](bloomFilter)(F, Cs)
  }

}
