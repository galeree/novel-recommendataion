import sun.security.util.PathList

name := "Dekd"

version := "1.0"

scalaVersion := "2.10.4"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "Recommender.jar" }