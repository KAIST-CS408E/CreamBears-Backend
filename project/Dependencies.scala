import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val elasticSearch = "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "6.4.2"
  lazy val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.5.17"
  lazy val akkaTest = "com.typesafe.akka" %% "akka-testkit" % "2.5.17" % Test
  lazy val crawler = RootProject(uri("git://github.com/KAIST-CS408E/CreamBears-Crawl.git"))
  lazy val extractor = RootProject(uri("git://github.com/KAIST-CS408E/CreamBears-Extractor.git"))
  lazy val jodaTime = "joda-time" % "joda-time" % "2.9.9"
}
