import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val crawler = RootProject(uri("git://github.com/CreamBears/kaist-portal-crawl.git"))
}
