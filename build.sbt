import Dependencies._

lazy val root = (project in file("."))
  .dependsOn(crawler)
  .settings(
    inThisBuild(List(
      organization := "services.xis.elastic",
      scalaVersion := "2.12.7",
      version      := "0.0"
    )),
    name := "xis-elastic",
    javacOptions ++= Seq("-encoding", "UTF-8"),
    libraryDependencies += scalaTest % Test,
    libraryDependencies += elasticSearch
  )
