import Dependencies._

ThisBuild / scalaVersion := "2.13.8"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "io.rlecomte"
ThisBuild / organizationName := "Romain Lecomte"

lazy val root = (project in file("."))
  .settings(
    name := "process-management-example",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "dev.zio" %% "zio" % "1.0.14",
    libraryDependencies += "dev.zio" %% "zio-streams" % "1.0.14"
  )

addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full)
