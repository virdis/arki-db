
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

case class Key(underlying: Long)   extends AnyVal
case class Page(underlying: Int)   extends AnyVal

/**
  * Represents the Address in the [[Page]] where the
  * [[PayloadBuffer]] is going to be added
  */
case class Offset(underlying: Int) extends AnyVal

case class SearchResult(key: Key, page: Page, offSet: Offset)
case class IndexElement(key: Key, page: Page, offSet: Offset)

object SearchResult {
  final val nfKey    = -1
  final val nfPage   = nfKey
  final val nfOffSet = nfKey

  final val NOT_FOUND = SearchResult(Key(nfKey), Page(nfPage), Offset(nfOffSet))
}