name := "akkaflow"
version := "2.9.8"
scalaVersion := "2.12.8"
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "oracle" at "https://maven.atlassian.com/3rdparty/"
resolvers += "cloudera Repository2" at "https://repository.cloudera.com/content/repositories/releases/"
resolvers += "hivei1" at "https://mvnrepository.com/artifact/org.apache.hive"
resolvers += "hive2" at "https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc"
resolvers += "hive3" at "https://mvnrepository.com/artifact/org.apache.hive/hive-exec"
resolvers += "spring Repository" at "http://repo.spring.io/plugins-release/"
resolvers += "cloudera Repository" at "https://repository.cloudera.com/artifactory/libs-release-local/"

libraryDependencies ++= { 
  val akkaVersion = "2.5.18"
  Seq(
    "org.scala-lang.modules" %% "scala-xml" % "1.1.1",
    "com.typesafe.akka" %% "akka-actor"      % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j"      % akkaVersion,
    "com.typesafe.akka" %% "akka-remote"     % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
    "com.typesafe.akka" %% "akka-distributed-data" % akkaVersion,
    "com.typesafe.akka" %% "akka-http" % "10.1.5",
    "com.typesafe.akka" %% "akka-testkit"    % akkaVersion   % "test",
    "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
    "ch.qos.logback"    %  "logback-classic" % "1.2.3",
    "org.scalatest"     %% "scalatest"       % "3.0.5"       % "test",
    "mysql" % "mysql-connector-java" % "5.1.42",
//    "com.github.philcali" %% "cronish" % "0.1.5",
    "com.frograms" %% "cronish" % "0.1.5-FROGRAMS",
    "org.json4s" %% "json4s-jackson" % "3.6.5",
    "org.apache.commons" % "commons-email" % "1.4",
    "org.apache.hadoop" % "hadoop-client" % "2.6.0",
    "org.apache.hive" % "hive-exec" % "1.1.1",
    "org.apache.hive" % "hive-jdbc" % "1.1.1",
    //"org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.13.3",
    //"org.apache.hive" % "hive-exec" % "1.1.0-cdh5.15.1",
    //"org.apache.hive" % "hive-exec" % "1.1.0-cdh5.3.6",
    //"org.apache.sqoop" % "sqoop" % "1.4.6.2.4.2.12-1",
    "com.oracle" % "ojdbc6" % "12.1.0.1-atlassian-hosted"
  )
}


maintainer := "492005267@qq.com"
import NativePackagerHelper._

enablePlugins(JavaServerAppPackaging)

scriptClasspath := Seq("../config/") ++ scriptClasspath.value

mappings in Universal ++= {
  directory("config") ++ contentOf("src/main/resources").toMap.mapValues("config/" + _)
}
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

