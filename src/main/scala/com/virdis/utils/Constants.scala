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

package com.virdis.utils

import com.kenai.jffi.{MemoryIO, PageManager}
import Tags._
object Constants {


  final val memoryManager       = MemoryIO.getInstance()
  final val pageManager         = PageManager.getInstance()
  final val PAGE_SIZE           = pageManager.pageSize().toInt
  final val SIXTY_FOUR_MB_BYTES = 67108864 // 64 MB
  final val INDEX_KEY_SIZE      = 16 // KEY:PAGENO:OFFSET
  // TODO ADD DOCUMENTATION FOR THIS CHOICE
  //https://www.di-mgt.com.au/bloom-calculator.html
  /**
    * Given any 3 parameters out of (n, m, p, k), compute the 4th; or, given any two of (n, m, p) compute the 3rd plus the optimum k.
    *
    * n=
    * 2169467
    * number of items in set
    * m=
    * number of bits in filter (optional form b^e, e.g. 2^16 = 65536)
    * k=
    * 5
    * number of hash functions
    * p=
    * 0.01
    * probability of a false positive, a real number 0 < p < 1
    */
  final val BLOOM_FILTER_BITS   = 21366654 // 2610 kB
  final val BLOOM_FILTER_HASHES = 5
  final val FOOTER_SIZE         = 60 // TIMESTAMP:MIN:MAX:DATABUFFEROFFSET:DATABUFFERSIZE:INDEXOFFSET:NUMBEROFKEYS:BFSTART:BLOCKNUMBER

  final val LONG_SIZE_IN_BYTES     = java.lang.Long.BYTES
  final val INT_SIZE_IN_BYTES      = java.lang.Integer.BYTES
  final val SHORT_SIZE_IN_BYTES    = java.lang.Short.BYTES
  final val BYTE_SIZE_IN_BYTES     = java.lang.Byte.BYTES
  final val TRUE_BYTES             = 1.toByte
  final val FALSE_BYTES            = 0.toByte
  final val XXHASH_SEED            = 0x9747b28b
  final val BLOOM_SEED             = 0x9747b28c
  final val HOME_DIRECTORY         = System.getProperty("user.home") // will work for unix and unix like OSes


  object Test {
    final val TEST_PG_SIZE    = 64 // bytes
    final val TEST_BLCK_SIZE  = 512 // bytes
    final val TEST_BF_SIZE    = 16 // bytes
    final val TEST_FOOTER_SIZE = 8
  }
}
