name := "edoc-streaming"

version := "0.2-SNAPSHOT"

organization := "io.github.dataramblers"

scalaVersion := "2.11.12"

val elastic4sVersion = "5.5.2"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.2",
      "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
      "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,
      "com.sksamuel.elastic4s" %% "elastic4s-json4s" % elastic4sVersion,
      "org.json4s" %% "json4s-native" % "3.5.3"
    )
  )

mainClass in assembly := Some("io.github.dataramblers.EdocStreaming")

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
  mainClass in(Compile, run),
  runner in(Compile, run)
).evaluated

// exclude Scala library from assembly
// assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
