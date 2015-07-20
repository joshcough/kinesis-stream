name := "kinesis-stream"

version := "1.0"

organization := "com.localytics"

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.9.3", "2.10.5", "2.11.7")

scalacOptions ++= Seq(
  "-language:implicitConversions"
 ,"-language:higherKinds"
 ,"-deprecation"
 ,"-encoding", "UTF-8" // yes, this is 2 args
 ,"-feature"
 ,"-unchecked"
 ,"-Xfatal-warnings"
 ,"-Xlint"
 ,"-Ywarn-adapted-args"
 ,"-Ywarn-dead-code"
 ,"-Ywarn-numeric-widen"
 ,"-Ywarn-value-discard"
 ,"-Xfuture"
)

parallelExecution := false

parallelExecution in Test := false

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest,
  "-oD"
 ,"-u", "target/test-reports"
 ,"-h", "target/test-reports"
)

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
 ,"Scalaz Bintray Repo" at "https://dl.bintray.com/scalaz/releases"
 ,Resolver.sonatypeRepo("releases")
)

libraryDependencies ++= Seq(
  "org.scalaz"        %% "scalaz-core"             % "7.1.3"
 ,"com.amazonaws"      % "amazon-kinesis-producer" % "0.9.0"
 ,"org.scalaz.stream" %% "scalaz-stream"           % "0.7.1a"
 ,"org.scalacheck"    %% "scalacheck"              % "1.12.4"  % "test"
 ,"org.mockito"        % "mockito-all"             % "1.9.5"   % "test"
 ,"org.scalatest"     %% "scalatest"               % "2.2.4"   % "test"
)

addCompilerPlugin("org.spire-math" % "kind-projector" % "0.6.2" cross CrossVersion.binary)