package com.virdis.models

import java.nio.ByteBuffer

//TODO
case class Record(key: RecordKey, payload: ByteBuffer, isDeleted: IsDeleted)
