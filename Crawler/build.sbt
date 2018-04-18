name := "Crawler"
 
version := "1.0" 
      
lazy val `crawler` = (project in file(".")).enablePlugins(PlayScala)

resolvers ++= Seq(
  "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"
)
      
scalaVersion := "2.12.2"

libraryDependencies ++= Seq( jdbc , ehcache , ws , specs2 % Test , guice,
  "org.jsoup" % "jsoup" % "1.11.2" from "file:///home/bahadir/Desktop/Crawler/jsoup-1.11.2.jar",
  "com.google.cloud" % "google-cloud-datastore" % "1.24.1",
  "com.google.cloud" % "google-cloud-pubsub" % "0.42.1-beta",
  "com.typesafe.play" %% "play-json" % "2.6.1",
  "com.google.cloud" % "google-cloud-storage" % "1.24.1"
)

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )  

      