name := "WorkflowSystem"
version := "1.0"
scalaVersion := "2.11.8"
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= {
  val akkaVersion = "2.4.8"
  Seq(
    "com.typesafe.akka" %% "akka-actor"      % akkaVersion,
    "com.typesafe.akka" %% "akka-http-core"  % akkaVersion, 
    "com.typesafe.akka" %% "akka-http-experimental"  % akkaVersion, 
    "com.typesafe.akka" %% "akka-http-spray-json-experimental"  % akkaVersion, 
    "com.typesafe.akka" %% "akka-slf4j"      % akkaVersion,
    "ch.qos.logback"    %  "logback-classic" % "1.1.3",
    "com.typesafe.akka" %% "akka-testkit"    % akkaVersion   % "test",
    "org.scalatest"     %% "scalatest"       % "2.2.0"       % "test",
    "com.typesafe.akka" %% "akka-remote"     % akkaVersion,
    "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
    "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
    "mysql" % "mysql-connector-java" % "5.1.+",
    //"io.kamon" % "sigar-loader" % "1.6.6-rev002",
    "com.github.philcali" %% "cronish" % "0.1.3",
    "org.json4s" % "json4s-jackson_2.11" % "3.5.0",
    "org.apache.commons" % "commons-email" % "1.4"
  )
}

//deploy base on sbt-native-packager
import NativePackagerHelper._
enablePlugins(JavaServerAppPackaging)
//mainClass in Compile := Some("com.kent.main.Master")
mainClass in Compile := Some("com.kent.main.Worker")
mappings in Universal ++= {
  // optional example illustrating how to copy additional directory
  directory("scripts") ++
  // copy configuration files to config directory
  contentOf("src/main/resources").toMap.mapValues("config/" + _)
}
// add 'config' directory first in the classpath of the start script,
// an alternative is to set the config file locations via CLI parameters
// when starting the application
scriptClasspath := Seq("../config/") ++ scriptClasspath.value