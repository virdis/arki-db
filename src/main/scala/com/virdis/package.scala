package com

import com.kenai.jffi.{MemoryIO, PageManager}

package object virdis {

  final val memoryManager   = MemoryIO.getInstance()
  final val pageManager     = PageManager.getInstance()

  final val pageSize: Long  = pageManager.pageSize()
}
