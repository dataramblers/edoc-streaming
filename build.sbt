resolvers in ThisBuild ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "edoc-streaming"

version := "0.1-SNAPSHOT"

organization := "io.github.dataramblers"

scalaVersion in ThisBuild := "2.11.11"

val flinkVersion = "1.3.2"
val elastic4sVersion = "5.5.2"
val circeVersion = "0.8.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-json4s" % elastic4sVersion,
  "org.json4s" %% "json4s-native" % "3.5.3"
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

mainClass in assembly := Some("io.github.dataramblers.Job")

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
  mainClass in(Compile, run),
  runner in(Compile, run)
).evaluated

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
