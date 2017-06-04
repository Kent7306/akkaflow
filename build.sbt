name := "akkaflow"
version := "2.3"
scalaVersion := "2.11.8"
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= {
  val akkaVersion = "2.4.16"
  Seq(
    "com.typesafe.akka" %% "akka-actor"      % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j"      % akkaVersion,
    "ch.qos.logback"    %  "logback-classic" % "1.1.3",
    "com.typesafe.akka" %% "akka-testkit"    % akkaVersion   % "test",
    "org.scalatest"     %% "scalatest"       % "2.2.0"       % "test",
    "com.typesafe.akka" %% "akka-remote"     % akkaVersion,
    "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
    "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
    "mysql" % "mysql-connector-java" % "5.1.42",
    "com.github.philcali" %% "cronish" % "0.1.3",
    "org.json4s" % "json4s-jackson_2.11" % "3.5.0",
    "org.apache.commons" % "commons-email" % "1.4",
    "com.typesafe.akka" %% "akka-http" % "10.0.6"
  )
}


import NativePackagerHelper._
enablePlugins(JavaServerAppPackaging)

mappings in Universal ++= {
  directory("config") ++ contentOf("src/main/resources").toMap.mapValues("config/" + _)
}
scriptClasspath := Seq("../config/") ++ scriptClasspath.value
mappings in Universal ++= {
    directory("tmp") ++ contentOf("tmp").toMap.mapValues("tmp/" + _)
}
mappings in Universal ++= { 
	directory("sbin") ++ contentOf("sbin").toMap.mapValues("sbin/" + _) 
}
mappings in Universal ++= {
	directory("config") ++ contentOf("config").toMap.mapValues("config/" + _) 
}
mappings in Universal ++= {
	directory("xmlconfig") ++ contentOf("xmlconfig").toMap.mapValues("xmlconfig/" + _) 
}

