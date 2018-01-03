name := "akkaflow"
version := "2.6.3"
scalaVersion := "2.11.8"
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "oracle" at "https://maven.atlassian.com/3rdparty/"
resolvers += "spring Repository" at "http://repo.spring.io/plugins-release/"
resolvers += "cloudera Repository" at "https://repository.cloudera.com/artifactory/libs-release-local/"

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
    "com.typesafe.akka" %% "akka-distributed-data-experimental" % akkaVersion,
    "com.typesafe.akka" %% "akka-http" % "10.0.6",
    "mysql" % "mysql-connector-java" % "5.1.42",
    "com.oracle" % "ojdbc6" % "12.1.0.1-atlassian-hosted" % Test,
    //"org.apache.hive" % "hive-jdbc" % "1.2.1",
    "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.13.1",
    //"org.apache.sqoop" % "sqoop" % "1.4.6.2.4.2.12-1",
    "com.github.philcali" %% "cronish" % "0.1.3",
    "org.json4s" % "json4s-jackson_2.11" % "3.5.0",
    "org.apache.commons" % "commons-email" % "1.4",
    "org.apache.hadoop" % "hadoop-client" % "2.6.0"
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
mappings in Universal ++= {
	directory("example") ++ contentOf("example").toMap.mapValues("example/" + _) 
}

