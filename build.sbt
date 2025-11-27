val scala3Version = "3.7.3"

lazy val root = project
  .in(file("."))
  .settings(
    name := "rssify",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    ThisBuild / organization := "org.github.sguzman",
    libraryDependencies ++= Seq(
      "com.lihaoyi" %% "os-lib" % "0.11.6",
      "org.typelevel" %% "cats-effect" % "3.6.3",
      "co.fs2" %% "fs2-core" % "3.12.2",
      "org.http4s" %% "http4s-ember-client" % "0.23.32",
      "org.tpolecat" %% "doobie-core" % "1.0.0-RC11",
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC11",
      "org.tpolecat" %% "doobie-log4cats" % "1.0.0-RC11",
      "org.xerial" % "sqlite-jdbc" % "3.51.0.0",
      "org.typelevel" %% "log4cats-slf4j" % "2.7.1",
      "ch.qos.logback" % "logback-classic" % "1.5.11",
      "com.indoorvivants" %% "toml" % "0.3.0",
      "org.typelevel" %% "munit-cats-effect" % "2.0.0" % Test
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:imports",
      "-Wunused:locals",
      "-Wunused:params",
      "-Wunused:privates",
      "-encoding",
      "UTF-8"
    )
  )
