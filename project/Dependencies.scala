import sbt._

object Dependencies {
  lazy val scalaTest            = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val scalaCheck           = "org.scalacheck" %% "scalacheck" % "1.14.0"
  lazy val catsEffect           = "org.typelevel" %% "cats-effect" % "1.2.0"
  lazy val jnrJffi              = "com.github.jnr" % "jnr-ffi" % "2.1.6"
  lazy val lz4                  = "org.lz4" % "lz4-java" % "1.5.0"
  lazy val scodec_bits          = "org.scodec" %% "scodec-bits" % "1.1.6"
  lazy val typeSafeConfig       = "com.typesafe" % "config" % "1.3.2"
  lazy val caffeineCache        = "com.github.ben-manes.caffeine" % "caffeine" % "2.7.0"
  lazy val catsCollection       = "org.typelevel" %% "cats-collections-core" % "0.7.0"
}
