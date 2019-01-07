import Dependencies._

lazy val commonDependencies = Seq(
  catsEffect,
  jnrJffi,
  lz4,
  scalaTest % Test
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
