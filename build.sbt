import sun.security.util.PathList
import _root_.sbtassembly.AssemblyPlugin.autoImport._
import _root_.sbtassembly.PathList

lazy val commonSettings = Seq(
  name := "Dekd"
  version := "1.0"
  scalaVersion := "2.10.5"
)
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

mainClass in Compile := Some("com.github.galeree.dekd")

libraryDependencies ++= {
  val akkaV       = "2.3.14"
  val akkaStreamV = "2.0.1"
  val scalaTestV  = "2.2.5"
  Seq(
    "org.apache.spark"  %% "spark-core"                           % "1.6.0" % "provided",
    "org.apache.spark"  %% "spark-mllib"                          % "1.6.0",
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case _                             => MergeStrategy.first
}