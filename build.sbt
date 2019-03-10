import Dependencies._

resolvers += "Sonatype Public" at "https://oss.sonatype.org/content/groups/public/"

lazy val commonDependencies = Seq(
  catsEffect,
  jnrJffi,
  lz4,
  scodec_bits,
  scodec_core,
  scalaTest % Test,
  scalaCheck % Test
)
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "arki-db",
    libraryDependencies ++= commonDependencies
  )

Test / fork := true
Test / javaOptions += "-Xmx4G"