name := "WorkflowSystem"
version := "1.0"
scalaVersion := "2.11.8"
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
//resolvers += "jsonlib" at "http://central.maven.org/maven2"

libraryDependencies ++= {
  val akkaVersion = "2.4.8" //<co id="akkaVersion"/>
  Seq(
    "com.typesafe.akka" %% "akka-actor"      % akkaVersion, //<co id="actorDep"/>
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
    "org.json4s" % "json4s-jackson_2.11" % "3.5.0"
    //"io.kamon" % "sigar-loader" % "1.6.6-rev002"
  )
}

//libraryDependencies += "net.sf.json-lib" % "json-lib" % "2.4" % "jdk15"
libraryDependencies += "com.github.philcali" %% "cronish" % "0.1.3"
