package com.virdis.writer

import com.virdis._
import java.nio.ByteBuffer

import cats.effect.Sync
import cats.effect.concurrent.Ref

abstract class BlockWriter[F[_]]()(implicit F: Sync[F]) {

  // TODO Document
  final case class PageAligned() {
    final val pgBuffer = Ref.of[F, ByteBuffer](ByteBuffer.allocateDirect(pageSize.toInt))

    def debug: F[Unit] =
      F.flatMap(pgBuffer) {
        refBuff => F.flatMap(refBuff.get) {
          buffer =>
            F.delay(println(s"PageAligned[pgBuffer=${buffer}]"))
        }
      }
    def position: F[Int] =
      F.flatMap(pgBuffer) {
        refBuff  => F.flatMap(refBuff.get) {
          buffer => F.point(buffer.position())
        }
      }
    def hasRemaining: F[Boolean] =
      F.flatMap(pgBuffer) {
        refBuff  => F.flatMap(refBuff.get) {
          buffer => F.point(buffer.hasRemaining)
        }
      }
    def remaining: F[Int] =
      F.flatMap(pgBuffer) {
        refBuff  => F.flatMap(refBuff.get) {
          buffer => F.point(buffer.remaining())
        }
      }
    // TODO : remember to flip data before calling put
    def put(data: ByteBuffer): F[Unit] =
      F.flatMap(pgBuffer) {
        refBuff     => F.flatMap(F.point(data)){
          toBeAdded => refBuff.modify[Unit]{
            buffer  =>(buffer.put(toBeAdded), ())
          }
        }
      }

    def nativeAddress: F[Long] =
      F.flatMap(pgBuffer) {
        refBuff => F.flatMap(refBuff.get) {
          buffer => F.delay(memoryManager.getDirectBufferAddress(buffer))
        }
      }
  }


}
