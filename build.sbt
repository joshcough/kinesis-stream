name := "kinesis-stream"

version := "1.0"

organization := "com.localytics"

scalaVersion := "2.11.7"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings", 
  "-Xlint",
  "-Ywarn-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture")

parallelExecution := false

parallelExecution in Test := false

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest,
  "-oD",
  "-u", "target/test-reports",
  "-h", "target/test-reports"
)

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Scalaz Bintray Repo" at "https://dl.bintray.com/scalaz/releases"
)

libraryDependencies ++= Seq(
  "org.scalaz" %% "scalaz-core" % "7.1.0",
  "org.scalacheck" %% "scalacheck" % "1.11.6" % "test",
  "com.amazonaws" % "amazon-kinesis-producer" % "0.9.0",
  "org.scalaz.stream" %% "scalaz-stream" % "0.7.1a"
)

