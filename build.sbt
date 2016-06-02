
lazy val root = (project in file(".")).
  settings(
    name := "dekd_recommend",
    version := "1.0",
    scalaVersion := "2.10.5",
    mainClass in Compile := Some("recommend"),
    assemblyJarName in assembly := "dekd_recommend.jar"
  )

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"  % "1.6.1"  % "provided",
  "org.apache.spark"  %% "spark-mllib" % "1.6.1"
)

// META_INF discarding
assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}
