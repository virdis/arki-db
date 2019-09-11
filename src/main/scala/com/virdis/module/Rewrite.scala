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

package com.virdis.module

import com.virdis.hashing.Hasher
import com.virdis.io.BlockWriter
import com.virdis.search.Search
import com.virdis.search.inmemory.InMemoryMapSearch
import com.virdis.utils.Config
import net.jpountz.xxhash.XXHash64

case class Rewrite[F[_]](
                  config: Option[Config]           = None,
                  hasher: Option[Hasher[XXHash64]] = None,
                  writer: Option[BlockWriter[F]]   = None,
                  search: Option[Search[F]]        = None,
                  inMMSearch: Option[InMemoryMapSearch[F]] = None
                  )
