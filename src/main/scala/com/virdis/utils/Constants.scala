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

object Constants {


  final val memoryManager       = MemoryIO.getInstance()
  final val pageManager         = PageManager.getInstance()
  final val PAGE_SIZE           = 64 //pageManager.pageSize()
  final val SIXTY_FOUR_MB_BYTES = 67108864 // 64 MB
  final val INDEX_KEY_SIZE      = 16 // KEY:PAGENO:OFFSET
  final val BLOOM_FILTER_SIZE   = 3145728 // 3MB
  final val FOOTER_SIZE         = 48 // MIN:MAX:INDEXSTART:INDEXEND:BFSTART:BLOCKSTART

  final val LONG_SIZE_IN_BYTES     = java.lang.Long.BYTES
  final val INT_SIZE_IN_BYTES      = java.lang.Integer.BYTES
  final val SHORT_SIZE_IN_BYTES    = java.lang.Short.BYTES
  final val BYTE_SIZE_IN_BYTES     = java.lang.Byte.BYTES
}
