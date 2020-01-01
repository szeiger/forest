scalaVersion in Global := "2.13.1"

cancelable in Global := true

val hedgehogVersion = "84f11d82ae95859633927305cbc1e3e27e181225"

lazy val main = (project in file("main"))
  .settings(
    scalacOptions ++= Seq("-feature", "-opt:l:inline", "-opt-inline-from:scala.**,forest.**"),
    fork in Test := true,
    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
    libraryDependencies ++= Seq(
      "qa.hedgehog" %% "hedgehog-core" % hedgehogVersion % "test",
      "qa.hedgehog" %% "hedgehog-runner" % hedgehogVersion % "test",
      "qa.hedgehog" %% "hedgehog-sbt" % hedgehogVersion % "test"
    ),
    resolvers += "bintray-scala-hedgehog" at "https://dl.bintray.com/hedgehogqa/scala-hedgehog",
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v"),
    testFrameworks += TestFramework("hedgehog.sbt.Framework")
  )

lazy val bench = (project in file("bench"))
  .dependsOn(main)
  .enablePlugins(JmhPlugin)
  .settings(
    scalacOptions ++= Seq("-feature", "-opt:l:inline", "-opt-inline-from:scala.**,forest.**")
  )
