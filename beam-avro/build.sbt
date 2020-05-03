scalaVersion := "2.12.10"
version := "0.1.0-SNAPSHOT"

val beamVersion = "2.20.0"

lazy val root = (project in file("."))
  .settings(
    name := "beam-avro-scala",
    libraryDependencies ++= Seq(
      "org.apache.beam"    % "beam-sdks-java-core"                                  % beamVersion,
      "org.apache.beam"    % "beam-sdks-java-io-google-cloud-platform"              % beamVersion,
      "org.apache.beam"    % "beam-sdks-java-extensions-google-cloud-platform-core" % beamVersion,
      "org.apache.beam"    % "beam-runners-google-cloud-dataflow-java"              % beamVersion,
      "org.apache.commons" % "commons-lang3"                                        % "3.8.1",
      "org.apache.beam"    % "beam-runners-direct-java"                             % beamVersion,
      "org.apache.beam"    % "beam-sdks-java-extensions-sorter"                     % beamVersion,
      "org.apache.beam"    % "beam-sdks-java-io-jdbc"                               % beamVersion,
      "com.google.guava"   % "guava"                                                % "29.0-jre",
      "io.spray"           %% "spray-json"                                          % "1.3.5"
    )
  )

// https://github.com/julianpeeters/sbt-avrohugger
sourceGenerators in Compile += (avroScalaGenerateSpecific in Compile).taskValue
avroSpecificSourceDirectories in Compile += (sourceDirectory in Compile).value / "avsc"
avroSpecificScalaSource in Compile := (sourceDirectory in Compile).value / "scala"
avroScalaSpecificCustomNamespace in Compile := Map("com.google.cloud.solutions.beamavro.beans" -> "avro")
