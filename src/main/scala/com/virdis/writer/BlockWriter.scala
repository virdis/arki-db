package com.virdis.writer

import com.virdis._
import java.nio.ByteBuffer

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.instances.unit

trait BlockWriter[F[_]] {

  // TODO Document
  final case class PageAligned[F[_]]()(implicit F: Sync[F]) {
    final val pgBuffer = Ref.of[F, ByteBuffer](ByteBuffer.allocateDirect(pageSize.toInt))

    def debug: F[Unit] =
      F.flatMap(pgBuffer) {
        refBuff => F.flatMap(refBuff.get) {
          buffer =>
            F.delay(println(s"PageAligned[pgBuffer=${buffer}]"))
        }
      }

    def size: F[Int] =
      F.flatMap(pgBuffer) {
        refBuff  => F.flatMap(refBuff.get) {
          buffer => F.point(buffer.position())
        }
      }
    // TODO : remember to flip data before calling put
    def put(data: ByteBuffer): F[Unit] =
      F.flatMap(pgBuffer) {
        refBuff     => F.flatMap(F.point(data)){
          toBeAdded => refBuff.modify[Unit]{
            bff =>(bff.put(toBeAdded), ())
          }
        }
      }
  }


}
