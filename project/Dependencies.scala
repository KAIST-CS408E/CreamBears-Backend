import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val elasticSearch = "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "6.4.2"
  lazy val crawler = RootProject(uri("git://github.com/CreamBears/kaist-portal-crawl.git"))
}
