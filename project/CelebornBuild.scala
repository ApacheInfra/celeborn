/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.file.Files
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale

import scala.util.Properties
import scala.xml._
import scala.xml.transform._

import com.github.sbt.git.SbtGit.GitKeys._
import org.openapitools.generator.sbt.plugin.OpenApiGeneratorPlugin
import org.openapitools.generator.sbt.plugin.OpenApiGeneratorPlugin.autoImport._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtprotoc.ProtocPlugin.autoImport._

import sbt._
import sbt.Keys._
import Utils._
import CelebornCommonSettings._
// import sbt.Keys.streams

object Dependencies {

  val zstdJniVersion = sparkClientProjects.map(_.zstdJniVersion).getOrElse("1.5.2-1")
  val lz4JavaVersion = sparkClientProjects.map(_.lz4JavaVersion).getOrElse("1.8.0")

  // Dependent library versions
  val apLoaderVersion = "3.0-8"
  val commonsCompressVersion = "1.4.1"
  val commonsCryptoVersion = "1.0.0"
  val commonsIoVersion = "2.13.0"
  val commonsLoggingVersion = "1.1.3"
  val commonsLang3Version = "3.17.0"
  val findbugsVersion = "1.3.9"
  val guavaVersion = "33.1.0-jre"
  val hadoopVersion = "3.3.6"
  val awsVersion = "1.12.367"
  val junitInterfaceVersion = "0.13.3"
  // don't forget update `junitInterfaceVersion` when we upgrade junit
  val junitVersion = "4.13.2"
  val leveldbJniVersion = "1.8"
  val log4j2Version = "2.17.2"
  val jdkToolsVersion = "0.1"
  val metricsVersion = "4.2.25"
  val mockitoVersion = "4.11.0"
  val nettyVersion = "4.1.109.Final"
  val ratisVersion = "3.1.1"
  val roaringBitmapVersion = "1.0.6"
  val rocksdbJniVersion = "9.5.2"
  val jacksonVersion = "2.15.3"
  val jakartaActivationApiVersion = "1.2.1"
  val scalatestMockitoVersion = "1.17.14"
  val scalatestVersion = "3.2.16"
  val slf4jVersion = "1.7.36"
  val snakeyamlVersion = "2.2"
  val snappyVersion = "1.1.10.5"
  val mybatisVersion = "3.5.15"
  val hikaricpVersion = "4.0.3"
  val h2Version = "2.2.224"
  val swaggerVersion = "2.2.1"
  val swaggerUiVersion = "4.9.1"
  val jerseyVersion = "2.39.1"
  val jettyVersion = "9.4.52.v20230823"
  val jakartaServeletApiVersion = "4.0.4"
  val openApiToolsJacksonBindNullableVersion = "0.2.6"
  val httpClient5Version = "5.3.1"
  val httpCore5Version = "5.2.4"
  val jakartaAnnotationApiVersion = "1.3.5"
  val picocliVersion = "4.7.6"

  // For SSL support
  val bouncycastleVersion = "1.77"

  // Versions for proto
  val protocVersion = "3.21.7"
  val protoVersion = "3.21.7"

  val apLoader = "me.bechberger" % "ap-loader-all" % apLoaderVersion
  val commonsCompress = "org.apache.commons" % "commons-compress" % commonsCompressVersion
  val commonsCrypto = "org.apache.commons" % "commons-crypto" % commonsCryptoVersion excludeAll(
    ExclusionRule("net.java.dev.jna", "jna"))
  val commonsIo = "commons-io" % "commons-io" % commonsIoVersion
  val commonsLang3 = "org.apache.commons" % "commons-lang3" % commonsLang3Version
  val commonsLogging = "commons-logging" % "commons-logging" % commonsLoggingVersion
  val jdkTools = "com.github.olivergondza" % "maven-jdk-tools-wrapper" % jdkToolsVersion
  val findbugsJsr305 = "com.google.code.findbugs" % "jsr305" % findbugsVersion
  val guava = "com.google.guava" % "guava" % guavaVersion excludeAll(
    ExclusionRule("org.checkerframework", "checker-qual"),
    ExclusionRule("org.codehaus.mojo", "animal-sniffer-annotations"),
    ExclusionRule("com.google.errorprone", "error_prone_annotations"),
    ExclusionRule("com.google.guava", "listenablefuture"),
    ExclusionRule("com.google.j2objc", "j2objc-annotations"))
  val hadoopClientApi = "org.apache.hadoop" % "hadoop-client-api" % hadoopVersion
  val hadoopClientRuntime = "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion
  val hadoopMapreduceClientApp = "org.apache.hadoop" % "hadoop-mapreduce-client-app" % hadoopVersion excludeAll(
    ExclusionRule("io.netty", "netty-transport-native-epoll"),
    ExclusionRule("com.google.guava", "guava"),
    ExclusionRule("com.fasterxml.jackson.core", "jackson-annotations"),
    ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
    ExclusionRule("jakarta.activation", "jakarta.activation-api"),
    ExclusionRule("jline", "jline"),
    ExclusionRule("log4j", "log4j"),
    ExclusionRule("org.slf4j", "slf4j-log4j12"))
  val hadoopAws = "org.apache.hadoop" % "hadoop-aws" % hadoopVersion
  val awsClient = "com.amazonaws" % "aws-java-sdk-bundle" % awsVersion
  val ioDropwizardMetricsCore = "io.dropwizard.metrics" % "metrics-core" % metricsVersion
  val ioDropwizardMetricsGraphite = "io.dropwizard.metrics" % "metrics-graphite" % metricsVersion excludeAll (
    ExclusionRule("com.rabbitmq", "amqp-client"))
  val ioDropwizardMetricsJvm = "io.dropwizard.metrics" % "metrics-jvm" % metricsVersion
  val ioNetty = "io.netty" % "netty-all" % nettyVersion excludeAll(
    ExclusionRule("io.netty", "netty-handler-ssl-ocsp"))
  val leveldbJniGroup = if (System.getProperty("os.name").startsWith("Linux")
    && System.getProperty("os.arch").equals("aarch64")) {
    // use org.openlabtesting.leveldbjni on aarch64 platform except MacOS
    // org.openlabtesting.leveldbjni requires glibc version 3.4.21
    "org.openlabtesting.leveldbjni"
  } else {
    "org.fusesource.leveldbjni"
  }
  val leveldbJniAll = leveldbJniGroup % "leveldbjni-all" % leveldbJniVersion
  val log4j12Api = "org.apache.logging.log4j" % "log4j-1.2-api" % log4j2Version
  val log4jSlf4jImpl = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j2Version
  val lz4Java = "org.lz4" % "lz4-java" % lz4JavaVersion
  val protobufJava = "com.google.protobuf" % "protobuf-java" % protoVersion
  val ratisClient = "org.apache.ratis" % "ratis-client" % ratisVersion
  val ratisCommon = "org.apache.ratis" % "ratis-common" % ratisVersion
  val ratisGrpc = "org.apache.ratis" % "ratis-grpc" % ratisVersion
  val ratisMetricsDefault = "org.apache.ratis" % "ratis-metrics-default" % ratisVersion
  val ratisNetty = "org.apache.ratis" % "ratis-netty" % ratisVersion
  val ratisServer = "org.apache.ratis" % "ratis-server" % ratisVersion
  val ratisShell = "org.apache.ratis" % "ratis-shell" % ratisVersion excludeAll(
    ExclusionRule("org.slf4j", "slf4j-simple"))
  val roaringBitmap = "org.roaringbitmap" % "RoaringBitmap" % roaringBitmapVersion
  val rocksdbJni = "org.rocksdb" % "rocksdbjni" % rocksdbJniVersion
  val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
  val jacksonCore = "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion
  val jacksonAnnotations = "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion
  val jacksonModule = "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
  val jacksonDataTypeJsr310 = "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion
  val jacksonDataFormatYam = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion
  val jacksonJaxrsBase = "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-base" % jacksonVersion
  val jacksonJaxrsJsonProvider = "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % jacksonVersion excludeAll (
    ExclusionRule("jakarta.activation", "jakarta.activation-api"))
  val jacksonModuleJaxbAnnotations = "com.fasterxml.jackson.module" % "jackson-module-jaxb-annotations" % jacksonVersion excludeAll (
    ExclusionRule("jakarta.activation", "jakarta.activation-api"))
  val jakartaActivationApi = "jakarta.activation" % "jakarta.activation-api" % jakartaActivationApiVersion
  val scalaReflect = "org.scala-lang" % "scala-reflect" % projectScalaVersion
  val slf4jApi = "org.slf4j" % "slf4j-api" % slf4jVersion
  val slf4jJulToSlf4j = "org.slf4j" % "jul-to-slf4j" % slf4jVersion
  val slf4jJclOverSlf4j = "org.slf4j" % "jcl-over-slf4j" % slf4jVersion
  val snakeyaml = "org.yaml" % "snakeyaml" % snakeyamlVersion
  val snappyJava = "org.xerial.snappy" % "snappy-java" % snappyVersion
  val zstdJni = "com.github.luben" % "zstd-jni" % zstdJniVersion
  val mybatis = "org.mybatis" % "mybatis" % mybatisVersion
  val hikaricp = "com.zaxxer" % "HikariCP" % hikaricpVersion
  val jettyServer = "org.eclipse.jetty" % "jetty-server" % jettyVersion excludeAll(
    ExclusionRule("javax.servlet", "javax.servlet-api"))
  val jettyServlet = "org.eclipse.jetty" % "jetty-servlet" % jettyVersion excludeAll(
    ExclusionRule("javax.servlet", "javax.servlet-api"))
  val jettyProxy = "org.eclipse.jetty" % "jetty-proxy" % jettyVersion
  val jakartaServletApi = "jakarta.servlet" % "jakarta.servlet-api" % jakartaServeletApiVersion
  val jerseyServer = "org.glassfish.jersey.core" % "jersey-server" % jerseyVersion excludeAll(
    ExclusionRule("jakarta.xml.bind", "jakarta.xml.bind-api"))
  val jerseyContainerServletCore = "org.glassfish.jersey.containers" % "jersey-container-servlet-core" % jerseyVersion
  val jerseyHk2 = "org.glassfish.jersey.inject" % "jersey-hk2" % jerseyVersion
  val jerseyMediaJsonJackson = "org.glassfish.jersey.media" % "jersey-media-json-jackson" % jerseyVersion
  val jerseyMediaMultipart = "org.glassfish.jersey.media" % "jersey-media-multipart" % jerseyVersion
  val swaggerJaxrs2 = "io.swagger.core.v3" % "swagger-jaxrs2" %swaggerVersion excludeAll(
    ExclusionRule("com.sun.activation", "jakarta.activation"),
    ExclusionRule("org.javassist", "javassist"),
    ExclusionRule("jakarta.activation", "jakarta.activation-api"))
  val swaggerUi = "org.webjars" % "swagger-ui" % swaggerUiVersion
  val openApiToolsJacksonBindNullable = "org.openapitools" % "jackson-databind-nullable" % openApiToolsJacksonBindNullableVersion excludeAll(
    ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"))
  val httpClient5 = "org.apache.httpcomponents.client5" % "httpclient5" % httpClient5Version
  val httpCore5 = "org.apache.httpcomponents.core5" % "httpcore5" % httpCore5Version
  val httpCore5H2 = "org.apache.httpcomponents.core5" % "httpcore5-h2" % httpCore5Version
  val jakartaAnnotationApi = "jakarta.annotation" % "jakarta.annotation-api" % jakartaAnnotationApiVersion

  // Test dependencies
  // https://www.scala-sbt.org/1.x/docs/Testing.html
  val junitInterface = "com.github.sbt" % "junit-interface" % junitInterfaceVersion
  val junit = "junit" % "junit" % junitVersion
  val mockitoCore = "org.mockito" % "mockito-core" % mockitoVersion
  val mockitoInline = "org.mockito" % "mockito-inline" % mockitoVersion
  val scalatestMockito = "org.mockito" %% "mockito-scala-scalatest" % scalatestMockitoVersion
  val scalatest = "org.scalatest" %% "scalatest" % scalatestVersion
  val h2 = "com.h2database" % "h2" % h2Version
  val jerseyTestFrameworkCore = "org.glassfish.jersey.test-framework" % "jersey-test-framework-core" % jerseyVersion
  val jerseyTestFrameworkProviderJetty = "org.glassfish.jersey.test-framework.providers" % "jersey-test-framework-provider-jetty" % jerseyVersion excludeAll(
    ExclusionRule("org.eclipse.jetty", "jetty-util"),
    ExclusionRule("org.eclipse.jetty", "jetty-continuation"))

  // SSL support
  val bouncycastleBcprovJdk18on = "org.bouncycastle" % "bcprov-jdk18on" % bouncycastleVersion % "test"
  val bouncycastleBcpkixJdk18on = "org.bouncycastle" % "bcpkix-jdk18on" % bouncycastleVersion % "test"

  val picocli = "info.picocli" % "picocli" % picocliVersion
}

object CelebornCommonSettings {

  // Scala versions
  val SCALA_2_11_12 = "2.11.12"
  val SCALA_2_12_10 = "2.12.10"
  val SCALA_2_12_15 = "2.12.15"
  val SCALA_2_12_17 = "2.12.17"
  val SCALA_2_12_18 = "2.12.18"
  val scala213 = "2.13.5"
  val ALL_SCALA_VERSIONS = Seq(SCALA_2_11_12, SCALA_2_12_10, SCALA_2_12_15, SCALA_2_12_17, SCALA_2_12_18, scala213)

  val DEFAULT_SCALA_VERSION = SCALA_2_12_18

  val projectScalaVersion = defaultScalaVersion()

  scalaVersion := projectScalaVersion

  autoScalaLibrary := false

  // crossScalaVersions must be set to Nil on the root project
  crossScalaVersions := Nil

  lazy val commonSettings = Seq(
    organization := "org.apache.celeborn",
    scalaVersion := projectScalaVersion,
    crossScalaVersions := ALL_SCALA_VERSIONS,
    fork := true,
    scalacOptions ++= Seq("-target:jvm-1.8"),
    javacOptions ++= Seq("-encoding", UTF_8.name(), "-source", "1.8", "-g"),
    Compile / packageBin / packageOptions +=  Package.ManifestAttributes(
      "Build-Jdk-Spec" -> System.getProperty("java.version"),
      "Build-Revision" -> gitHeadCommit.value.getOrElse("N/A"),
      "Build-Branch" -> gitCurrentBranch.value,
      "Build-Time" -> java.time.ZonedDateTime.now().format(java.time.format.DateTimeFormatter.ISO_DATE_TIME)),
  
    // -target cannot be passed as a parameter to javadoc. See https://github.com/sbt/sbt/issues/355
    Compile / compile / javacOptions ++= Seq("-target", "1.8"),

    dependencyOverrides := Seq(
      Dependencies.commonsLogging,
      Dependencies.findbugsJsr305,
      Dependencies.slf4jApi),

    // Make sure any tests in any project that uses Spark is configured for running well locally
    Test / javaOptions ++= Seq(
      "-Xmx4g",
      "-XX:+IgnoreUnrecognizedVMOptions",
      "--add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true"
    ),

    testOptions += Tests.Argument("-oF"),

    Test / testOptions += Tests.Argument("-oDF"),
    Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),

    // Don't execute in parallel since we can't have multiple Sparks in the same JVM
    Test / parallelExecution := false,

    javaOptions += "-Xmx4g",

    // Configurations to speed up tests and reduce memory footprint
    Test / javaOptions ++= Seq(
      "-Xmx4g"
    ),

    Test / javaOptions ++= Seq(
      "-Dspark.shuffle.sort.io.plugin.class="
        + sys.props.getOrElse("spark.shuffle.plugin.class", "org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO"),
    ),

    Test / envVars += ("IS_TESTING", "1")
  )

  ////////////////////////////////////////////////////////
  //                 Release settings                   //
  ////////////////////////////////////////////////////////

  lazy val releaseSettings = Seq(
    publishMavenStyle := true,
    publishArtifact := true,
    Test / publishArtifact := false,
    credentials += {
      val host = publishTo.value.map {
        case repo: MavenRepo => scala.util.Try(new java.net.URL(repo.root)).map(_.getHost).getOrElse("repository.apache.org")
        case _ => "repository.apache.org"
      }.get

      Credentials(
        // Credentials matching is done using both: realm and host keys, sbt/sbt#2366 allows using
        // credential without a realm by providing an empty string for realm.
        "" /* realm */,
        host,
        sys.env.getOrElse("ASF_USERNAME", ""),
        sys.env.getOrElse("ASF_PASSWORD", ""))
    },
    publishTo := {
      if (isSnapshot.value) {
        val publishUrl = sys.env.getOrElse("SONATYPE_SNAPSHOTS_URL", "https://repository.apache.org/content/repositories/snapshots")
        Some(("snapshots" at publishUrl).withAllowInsecureProtocol(true))
      } else {
        val publishUrl = sys.env.getOrElse("SONATYPE_RELEASES_URL", "https://repository.apache.org/service/local/staging/deploy/maven2")
        Some(("releases" at publishUrl).withAllowInsecureProtocol(true))
      }
    },
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0")),
    scmInfo := Some(
      ScmInfo(
        url("https://celeborn.apache.org/"),
          "scm:git:https://github.com/apache/celeborn.git",
          "scm:git:git@github.com:apache/celeborn.git"))

  )

  lazy val protoSettings = Seq(
    // Setting version for the protobuf compiler
    PB.protocVersion := Dependencies.protocVersion,
    // set proto sources path
    Compile / PB.protoSources := Seq(sourceDirectory.value / "main" / "proto"),
    Compile / PB.targets := Seq(PB.gens.java(Dependencies.protocVersion) -> (Compile / sourceManaged).value)
  )

  lazy val commonUnitTestDependencies = Seq(
    Dependencies.mockitoCore % "test",
    Dependencies.scalatest % "test",
    Dependencies.junit % "test",
    // https://www.scala-sbt.org/1.x/docs/Testing.html
    Dependencies.junitInterface % "test")
}

object CelebornBuild extends sbt.internal.BuildDef {
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    Seq(
      CelebornOpenApi.openApiClientMasterGenerate,
      CelebornOpenApi.openApiClientWorkerGenerate,
      CelebornOpenApi.openApiClient,
      CelebornSpi.spi,
      CelebornCommon.common,
      CelebornClient.client,
      CelebornService.service,
      CelebornWorker.worker,
      CelebornMaster.master,
      CelebornCli.cli) ++ maybeSparkClientModules ++ maybeFlinkClientModules ++ maybeMRClientModules ++ maybeWebModules
  }

  // ThisBuild / parallelExecution := false

  // scalaVersion := "2.11.12"

  // autoScalaLibrary := false

  crossScalaVersions := Nil

  // load user-defined Profiles
  // loadProfiles()
}

object Utils {
  val profiles = {
    val profiles = Properties.envOrNone("SBT_MAVEN_PROFILES")
      .orElse(Properties.propOrNone("sbt.maven.profiles")) match {
        case None => Seq("sbt")
        case Some(v) =>
          v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
      }
      if (profiles.contains("jdwp-test-debug")) {
        sys.props.put("test.jdwp.enabled", "true")
      }
      profiles
  }
  val SPARK_VERSION = profiles.filter(_.startsWith("spark")).headOption

  lazy val sparkClientProjects = SPARK_VERSION match {
    case Some("spark-2.4") => Some(Spark24)
    case Some("spark-3.0") => Some(Spark30)
    case Some("spark-3.1") => Some(Spark31)
    case Some("spark-3.2") => Some(Spark32)
    case Some("spark-3.3") => Some(Spark33)
    case Some("spark-3.4") => Some(Spark34)
    case Some("spark-3.5") => Some(Spark35)
    case _ => None
  }

  lazy val maybeSparkClientModules: Seq[Project] = sparkClientProjects.map(_.modules).getOrElse(Seq.empty)

  val FLINK_VERSION = profiles.filter(_.startsWith("flink")).headOption

  lazy val flinkClientProjects = FLINK_VERSION match {
    case Some("flink-1.14") => Some(Flink114)
    case Some("flink-1.15") => Some(Flink115)
    case Some("flink-1.16") => Some(Flink116)
    case Some("flink-1.17") => Some(Flink117)
    case Some("flink-1.18") => Some(Flink118)
    case Some("flink-1.19") => Some(Flink119)
    case Some("flink-1.20") => Some(Flink120)
    case _ => None
  }

  lazy val maybeFlinkClientModules: Seq[Project] = flinkClientProjects.map(_.modules).getOrElse(Seq.empty)

  val MR_VERSION = profiles.filter(_.startsWith("mr")).headOption

  lazy val mrClientProjects = MR_VERSION match {
    case Some("mr") => Some(MRClientProjects)
    case _ => None
  }

  lazy val maybeMRClientModules: Seq[Project] = mrClientProjects.map(_.modules).getOrElse(Seq.empty)

  val WEB_VERSION = profiles.filter(_.startsWith("web")).headOption

  lazy val webProjects = WEB_VERSION match {
    case Some("web") => Some(WebProjects)
    case _ => None
  }

  lazy val maybeWebModules: Seq[Project] = webProjects.map(_.modules).getOrElse(Seq.empty)

  def defaultScalaVersion(): String = {
    // 1. Inherit the scala version of the spark project
    // 2. if the spark profile not specified, using the DEFAULT_SCALA_VERSION
    val v = sparkClientProjects.map(_.sparkProjectScalaVersion).getOrElse(DEFAULT_SCALA_VERSION)
    require(ALL_SCALA_VERSIONS.contains(v), s"found not allow scala version: $v")
    v
  }

  /**
   * The deps for shaded clients are already packaged in the jar,
   * so we should not expose the shipped transitive deps.
   */
  def removeDependenciesTransformer: xml.Node => xml.Node = { node =>
    new RuleTransformer(new RewriteRule {
      override def transform(n: xml.Node): Seq[xml.Node] = n match {
        case e: Elem if e.label == "dependencies" =>
          Nil
        case _ =>
          n
      }
    }).transform(node).head
  }
}

object CelebornCli {
  lazy val cli = Project("celeborn-cli", file("cli"))
    .dependsOn(CelebornCommon.common % "test->test;compile->compile")
    .dependsOn(CelebornMaster.master % "test->test;compile->compile")
    .dependsOn(CelebornWorker.worker % "test->test;compile->compile")
    .dependsOn(CelebornOpenApi.openApiClient % "test->test;compile->compile")
    .settings (
      commonSettings,
      libraryDependencies ++= Seq(
        Dependencies.picocli
      ) ++ commonUnitTestDependencies
    )
}

object CelebornSpi {
  lazy val spi = Project("celeborn-spi", file("spi"))
    .settings(
      commonSettings,
      releaseSettings,
      crossPaths := false,
      Compile / doc / javacOptions := Seq("-encoding", UTF_8.name(), "-source", "1.8")
    )
}

object CelebornCommon {
  import scala.sys.process.Process
  def buildenv = Process(Seq("uname")).!!.trim.replaceFirst("[^A-Za-z0-9].*", "").toLowerCase
  def bashpath = Process(Seq("where", "bash")).!!.split("[\r\n]+").head.replace('\\', '/')
  lazy val buildInfoSettings = Seq(
    (Compile / resourceGenerators) += Def.task {
      val buildScript = baseDirectory.value + "/../build/celeborn-build-info"
      val targetDir = baseDirectory.value + "/target/extra-resources/"
      // support Windows build under cygwin/mingw64, etc
      val bash = buildenv match {
        case "cygwin" | "msys2" | "mingw64" | "clang64" => bashpath
        case _ => "bash"
      }
      val command = Seq(bash, buildScript, targetDir, version.value)
      Process(command).!!
      val propsFile = baseDirectory.value / "target" / "extra-resources" / "celeborn-version-info.properties"
      Seq(propsFile)
    }.taskValue
  )

  lazy val hadoopAwsDependencies = if(profiles.exists(_.startsWith("hadoop-aws"))){
    Seq(Dependencies.hadoopAws, Dependencies.awsClient)
  } else {
    Seq.empty
  }

  lazy val common = Project("celeborn-common", file("common"))
    .dependsOn(CelebornSpi.spi)
    .settings (
      commonSettings,
      buildInfoSettings,
      protoSettings,
      libraryDependencies ++= Seq(
        Dependencies.protobufJava,
        Dependencies.findbugsJsr305,
        Dependencies.guava,
        Dependencies.commonsIo,
        Dependencies.ioDropwizardMetricsCore,
        Dependencies.ioDropwizardMetricsGraphite,
        Dependencies.ioDropwizardMetricsJvm,
        Dependencies.ioNetty,
        Dependencies.commonsCrypto,
        Dependencies.commonsLang3,
        Dependencies.hadoopClientApi,
        Dependencies.hadoopClientRuntime,
        Dependencies.jdkTools,
        Dependencies.leveldbJniAll,
        Dependencies.roaringBitmap,
        Dependencies.scalaReflect,
        Dependencies.slf4jJclOverSlf4j,
        Dependencies.slf4jJulToSlf4j,
        Dependencies.slf4jApi,
        Dependencies.snakeyaml,
        Dependencies.snappyJava,
        Dependencies.jacksonModule,
        Dependencies.jacksonCore,
        Dependencies.jacksonDatabind,
        Dependencies.jacksonAnnotations,
        Dependencies.log4jSlf4jImpl % "test",
        Dependencies.log4j12Api % "test",
        // SSL support
        Dependencies.bouncycastleBcprovJdk18on,
        Dependencies.bouncycastleBcpkixJdk18on
      ) ++ commonUnitTestDependencies ++ hadoopAwsDependencies,

      Compile / sourceGenerators += Def.task {
        val file = (Compile / sourceManaged).value / "org" / "apache" / "celeborn" / "package.scala"
        streams.value.log.info(s"geneate version information file ${file.toPath}")
        IO.write(file,
          s"""package org.apache
             |
             |package object celeborn {
             |  val VERSION = "${version.value}"
             |}
             |""".stripMargin)
        Seq(file)
        // generate version task depends on PB generate to avoid concurrency generate source files
      }.dependsOn(Compile / PB.generate),

      // a task to show current profiles
      printProfiles := {
        val message = profiles.mkString("", " ", "")
        println("compile with profiles: %s".format(message))
      }
    )

    lazy val printProfiles = taskKey[Unit]("Prints Profiles")
}

object CelebornClient {
  lazy val client = Project("celeborn-client", file("client"))
    // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
    .dependsOn(CelebornCommon.common % "test->test;compile->compile")
    .settings (
      commonSettings,
      libraryDependencies ++= Seq(
        Dependencies.ioNetty,
        Dependencies.guava,
        Dependencies.lz4Java,
        Dependencies.zstdJni,
        Dependencies.commonsLang3,
        Dependencies.log4jSlf4jImpl % "test",
        Dependencies.log4j12Api % "test"
      ) ++ commonUnitTestDependencies
    )
}

object CelebornService {
  lazy val service = Project("celeborn-service", file("service"))
    .dependsOn(CelebornCommon.common % "test->test;compile->compile")
    .dependsOn(CelebornOpenApi.openApiClient)
    .settings (
      commonSettings,
      libraryDependencies ++= Seq(
        Dependencies.findbugsJsr305,
        Dependencies.commonsIo,
        Dependencies.ioNetty,
        Dependencies.commonsCrypto,
        Dependencies.slf4jApi,
        Dependencies.mybatis,
        Dependencies.hikaricp,
        Dependencies.jacksonDataFormatYam,
        Dependencies.swaggerJaxrs2,
        Dependencies.swaggerUi,
        Dependencies.jakartaServletApi,
        Dependencies.jerseyServer,
        Dependencies.jerseyContainerServletCore,
        Dependencies.jerseyHk2,
        Dependencies.jerseyMediaJsonJackson,
        Dependencies.jerseyMediaMultipart,
        Dependencies.jettyServer,
        Dependencies.jettyServlet,
        Dependencies.jettyProxy,
        Dependencies.log4jSlf4jImpl % "test",
        Dependencies.log4j12Api % "test",
        Dependencies.h2 % "test",
        Dependencies.jerseyTestFrameworkCore % "test",
        Dependencies.jerseyTestFrameworkProviderJetty % "test"
      ) ++ commonUnitTestDependencies
    )
}

object CelebornMaster {
  lazy val master = Project("celeborn-master", file("master"))
    .dependsOn(CelebornCommon.common)
    .dependsOn(CelebornCommon.common % "test->test;compile->compile")
    .dependsOn(CelebornService.service % "test->test;compile->compile")
    .settings (
      commonSettings,
      protoSettings,
      libraryDependencies ++= Seq(
        Dependencies.guava,
        Dependencies.protobufJava,
        Dependencies.ioNetty,
        Dependencies.hadoopClientApi,
        Dependencies.log4j12Api,
        Dependencies.log4jSlf4jImpl,
        Dependencies.ratisClient,
        Dependencies.ratisCommon,
        Dependencies.ratisGrpc,
        Dependencies.ratisMetricsDefault,
        Dependencies.ratisNetty,
        Dependencies.ratisServer,
        Dependencies.ratisShell,
        Dependencies.scalatestMockito % "test",
      ) ++ commonUnitTestDependencies
    )
}

object CelebornWorker {
  lazy val worker = Project("celeborn-worker", file("worker"))
    .dependsOn(CelebornService.service)
    .dependsOn(CelebornCommon.common % "test->test;compile->compile")
    .dependsOn(CelebornService.service % "test->test;compile->compile")
    .dependsOn(CelebornClient.client % "test->compile")
    .dependsOn(CelebornMaster.master % "test->compile")
    .settings (
      commonSettings,
      libraryDependencies ++= Seq(
        Dependencies.apLoader,
        Dependencies.guava,
        Dependencies.commonsIo,
        Dependencies.ioNetty,
        Dependencies.log4j12Api,
        Dependencies.log4jSlf4jImpl,
        Dependencies.leveldbJniAll,
        Dependencies.roaringBitmap,
        Dependencies.rocksdbJni,
        Dependencies.scalatestMockito % "test",
        Dependencies.jerseyTestFrameworkCore % "test",
        Dependencies.jerseyTestFrameworkProviderJetty % "test"
      ) ++ commonUnitTestDependencies
    )
}

////////////////////////////////////////////////////////
//                   Spark Client                     //
////////////////////////////////////////////////////////

object Spark24 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-2"
  val sparkClientProjectName = "celeborn-client-spark-2"
  val sparkClientShadedProjectPath = "client-spark/spark-2-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-2-shaded"

  // val jacksonVersion = "2.5.7"
  // val jacksonDatabindVersion = "2.6.7.3"
  val lz4JavaVersion = "1.4.0"
  val sparkProjectScalaVersion = "2.11.12"
  // scalaBinaryVersion
  // val scalaBinaryVersion = "2.11"
  val sparkVersion = "2.4.8"
  val zstdJniVersion = "1.4.4-3"

  override val includeColumnarShuffle: Boolean = false
}

object Spark30 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  val lz4JavaVersion = "1.7.1"
  val sparkProjectScalaVersion = "2.12.10"

  val sparkVersion = "3.0.3"
  val zstdJniVersion = "1.4.4-3"
}

object Spark31 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  val lz4JavaVersion = "1.7.1"
  val sparkProjectScalaVersion = "2.12.10"

  val sparkVersion = "3.1.3"
  val zstdJniVersion = "1.4.8-1"
}

object Spark32 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  val lz4JavaVersion = "1.7.1"
  val sparkProjectScalaVersion = "2.12.15"

  val sparkVersion = "3.2.4"
  val zstdJniVersion = "1.5.0-4"
}

object Spark33 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  // val jacksonVersion = "2.13.4"
  // val jacksonDatabindVersion = "2.13.4.2"
  val lz4JavaVersion = "1.8.0"
  val sparkProjectScalaVersion = "2.12.15"
  // scalaBinaryVersion
  // val scalaBinaryVersion = "2.12"
  val sparkVersion = "3.3.4"
  val zstdJniVersion = "1.5.2-1"
}

object Spark34 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  val lz4JavaVersion = "1.8.0"
  val sparkProjectScalaVersion = "2.12.17"

  val sparkVersion = "3.4.4"
  val zstdJniVersion = "1.5.2-5"
}

object Spark35 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  val lz4JavaVersion = "1.8.0"
  val sparkProjectScalaVersion = "2.12.18"

  val sparkVersion = "3.5.3"
  val zstdJniVersion = "1.5.5-4"

  override val sparkColumnarShuffleVersion: String = "3.5"
}

trait SparkClientProjects {

  val sparkClientProjectPath: String
  val sparkClientProjectName: String
  val sparkClientShadedProjectPath: String
  val sparkClientShadedProjectName: String

  val lz4JavaVersion: String
  val sparkProjectScalaVersion: String
  val sparkVersion: String
  val zstdJniVersion: String

  val includeColumnarShuffle: Boolean = true

  def modules: Seq[Project] = {
    val seq = Seq(sparkCommon, sparkClient, sparkIt, sparkGroup, sparkClientShade)
    if (includeColumnarShuffle) seq ++ Seq(sparkColumnarCommon, sparkColumnarShuffle) else seq
  }

  // for test only, don't use this group for any other projects
  lazy val sparkGroup = {
    val p = (project withId "celeborn-spark-group")
      .aggregate(sparkCommon, sparkClient, sparkIt)
    if (includeColumnarShuffle) {
      p.aggregate(sparkColumnarCommon, sparkColumnarShuffle)
    } else {
      p
    }
  }

  def sparkCommon: Project = {
    Project("celeborn-spark-common", file("client-spark/common"))
      .dependsOn(CelebornCommon.common)
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(CelebornClient.client % "test->test;compile->compile")
      .settings (
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
          "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
          "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests"
        ) ++ commonUnitTestDependencies
      )
  }

  def sparkClient: Project = {
    Project(sparkClientProjectName, file(sparkClientProjectPath))
      .dependsOn(CelebornCommon.common, sparkCommon)
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(CelebornClient.client % "test->test;compile->compile")
      .settings (
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
          "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        ) ++ commonUnitTestDependencies
      )
  }

  def sparkColumnarCommon: Project = {
    Project("celeborn-spark-3-columnar-common", file("client-spark/spark-3-columnar-common"))
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(sparkClient)
      .settings(
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        )
      )
  }

  val sparkColumnarShuffleVersion: String = "3"

  def sparkColumnarShuffle: Project = {
    Project("celeborn-spark-3-columnar-shuffle", file(s"client-spark/spark-$sparkColumnarShuffleVersion-columnar-shuffle"))
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(sparkColumnarCommon)
      .dependsOn(sparkClient % "test->test;compile->compile")
      .dependsOn(CelebornClient.client % "test")
      .settings(
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        ) ++ commonUnitTestDependencies ++ Seq(Dependencies.mockitoInline % "test")
      )
  }

  def sparkIt: Project = {
    Project("celeborn-spark-it", file("tests/spark-it"))
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(CelebornCommon.common % "test->test;compile->compile")
      .dependsOn(CelebornClient.client % "test->test;compile->compile")
      .dependsOn(CelebornMaster.master % "test->test;compile->compile")
      .dependsOn(CelebornWorker.worker % "test->test;compile->compile")
      .dependsOn(sparkClient % "test->test;compile->compile")
      .settings (
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-core" % sparkVersion % "test",
          "org.apache.spark" %% "spark-sql" % sparkVersion % "test",
          "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests" excludeAll(
            ExclusionRule("org.glassfish.jersey.inject", "*"),
            ExclusionRule("org.glassfish.jersey.core", "*")),
          "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests"
        ) ++ commonUnitTestDependencies
      )
  }

  def sparkClientShade: Project = {
    var p = Project(sparkClientShadedProjectName, file(sparkClientShadedProjectPath))
      .dependsOn(sparkClient)

    if (includeColumnarShuffle) {
      p = p.dependsOn(sparkColumnarShuffle)
    }

    p = p.disablePlugins(AddMetaInfLicenseFiles)
      .settings (
        commonSettings,
        releaseSettings,

        // align final shaded jar name with maven.
        (assembly / assemblyJarName) := {
          val extension = artifact.value.extension
          s"${moduleName.value}_${scalaBinaryVersion.value}-${version.value}.$extension"
        },

        (assembly / test) := { },

        (assembly / logLevel) := Level.Info,

        // Exclude `scala-library` from assembly.
        (assembly / assemblyPackageScala / assembleArtifact) := false,

        (assembly / assemblyExcludedJars) := {
          val cp = (assembly / fullClasspath).value
          cp filter { v =>
            val name = v.data.getName
            !(name.startsWith("celeborn-") ||
              name.startsWith("protobuf-java-") ||
              name.startsWith("guava-") ||
              name.startsWith("failureaccess-") ||
              name.startsWith("netty-") ||
              name.startsWith("commons-lang3-") ||
              name.startsWith("RoaringBitmap-"))
          }
        },

        (assembly / assemblyShadeRules) := Seq(
          ShadeRule.rename("com.google.protobuf.**" -> "org.apache.celeborn.shaded.com.google.protobuf.@1").inAll,
          ShadeRule.rename("com.google.common.**" -> "org.apache.celeborn.shaded.com.google.common.@1").inAll,
          ShadeRule.rename("io.netty.**" -> "org.apache.celeborn.shaded.io.netty.@1").inAll,
          ShadeRule.rename("org.apache.commons.**" -> "org.apache.celeborn.shaded.org.apache.commons.@1").inAll,
          ShadeRule.rename("org.roaringbitmap.**" -> "org.apache.celeborn.shaded.org.roaringbitmap.@1").inAll
        ),

        (assembly / assemblyMergeStrategy) := {
          case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
          // the LicenseAndNoticeMergeStrategy always picks the license/notice file from the current project
          case m @ ("META-INF/LICENSE" | "META-INF/NOTICE") => CustomMergeStrategy("LicenseAndNoticeMergeStrategy") { conflicts =>
            val entry = conflicts.head
            val projectLicenseFile = (Compile / resourceDirectory).value / entry.target
            val stream = () => new java.io.BufferedInputStream(new java.io.FileInputStream(projectLicenseFile))
            Right(Vector(JarEntry(entry.target, stream)))
          }
          case PathList(ps@_*) if Assembly.isLicenseFile(ps.last) => MergeStrategy.discard
          // Drop all proto files that are not needed as artifacts of the build.
          case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
          case m if m.toLowerCase(Locale.ROOT).startsWith("meta-inf/native-image") => MergeStrategy.discard
          // Drop netty jnilib
          case m if m.toLowerCase(Locale.ROOT).endsWith(".jnilib") => MergeStrategy.discard
          // rename netty native lib
          case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => CustomMergeStrategy.rename( _ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_x86_64.so" )
          case "META-INF/native/libnetty_transport_native_epoll_aarch_64.so" => CustomMergeStrategy.rename( _ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_aarch_64.so" )
          case _ => MergeStrategy.first
        },

        Compile / packageBin := assembly.value,
        pomPostProcess := removeDependenciesTransformer
      )
    p
  }
}

////////////////////////////////////////////////////////
//                   Flink Client                     //
////////////////////////////////////////////////////////

object Flink114 extends FlinkClientProjects {
  val flinkVersion = "1.14.6"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.14"
  val flinkClientProjectName = "celeborn-client-flink-1_14"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.14-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_14-shaded"

  override lazy val flinkStreamingDependency: ModuleID = "org.apache.flink" %% "flink-streaming-java" % flinkVersion % "test"
  override lazy val flinkClientsDependency: ModuleID = "org.apache.flink" %% "flink-clients" % flinkVersion % "test"
  override lazy val flinkRuntimeWebDependency: ModuleID = "org.apache.flink" %% "flink-runtime-web" % flinkVersion % "test"
}

object Flink115 extends FlinkClientProjects {
  val flinkVersion = "1.15.4"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.15"
  val flinkClientProjectName = "celeborn-client-flink-1_15"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.15-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_15-shaded"
}

object Flink116 extends FlinkClientProjects {
  val flinkVersion = "1.16.3"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.16"
  val flinkClientProjectName = "celeborn-client-flink-1_16"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.16-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_16-shaded"
}

object Flink117 extends FlinkClientProjects {
  val flinkVersion = "1.17.2"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.17"
  val flinkClientProjectName = "celeborn-client-flink-1_17"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.17-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_17-shaded"
}

object Flink118 extends FlinkClientProjects {
  val flinkVersion = "1.18.1"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.18"
  val flinkClientProjectName = "celeborn-client-flink-1_18"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.18-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_18-shaded"
}

object Flink119 extends FlinkClientProjects {
  val flinkVersion = "1.19.1"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.19"
  val flinkClientProjectName = "celeborn-client-flink-1_19"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.19-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_19-shaded"
}

object Flink120 extends FlinkClientProjects {
  val flinkVersion = "1.20.0"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.20"
  val flinkClientProjectName = "celeborn-client-flink-1_20"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.20-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_20-shaded"
}

trait FlinkClientProjects {

  val flinkVersion: String

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath: String
  val flinkClientProjectName: String
  val flinkClientShadedProjectPath: String
  val flinkClientShadedProjectName: String

  lazy val flinkStreamingDependency: ModuleID = "org.apache.flink" % "flink-streaming-java" % flinkVersion % "test"
  lazy val flinkClientsDependency: ModuleID = "org.apache.flink" % "flink-clients" % flinkVersion % "test"
  lazy val flinkRuntimeWebDependency: ModuleID = "org.apache.flink" % "flink-runtime-web" % flinkVersion % "test"

  def modules: Seq[Project] = Seq(flinkCommon, flinkClient, flinkIt, flinkGroup, flinkClientShade)

  // for test only, don't use this group for any other projects
  lazy val flinkGroup = (project withId "celeborn-flink-group")
    .aggregate(flinkCommon, flinkClient, flinkIt)

  // get flink major version. e.g:
  //   1.20.0 -> 1.20
  //   1.19.1 -> 1.19
  //   1.18.1 -> 1.18
  //   1.17.2 -> 1.17
  //   1.15.4 -> 1.15
  //   1.14.6 -> 1.14
  lazy val flinkMajorVersion: String = flinkVersion.split("\\.").take(2).reduce(_ + "." + _)

  // the output would be something like: celeborn-client-flink-1.17-shaded_2.12-0.4.0-SNAPSHOT.jar
  def flinkClientShadeJarName(
      revision: String,
      artifact: Artifact,
      scalaBinaryVersionString: String): String =
    s"celeborn-client-flink-$flinkMajorVersion-shaded_$scalaBinaryVersionString" + "-" + revision + "." + artifact.extension

  def flinkCommon: Project = {
    Project("celeborn-flink-common", file("client-flink/common"))
      .dependsOn(CelebornCommon.common, CelebornClient.client)
      .settings (
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.flink" % "flink-runtime" % flinkVersion % "provided"
        ) ++ commonUnitTestDependencies
      )
  }

  def flinkClient: Project = {
    Project(flinkClientProjectName, file(flinkClientProjectPath))
      .dependsOn(CelebornCommon.common, CelebornClient.client, flinkCommon)
      .settings (
        commonSettings,

        moduleName := s"celeborn-client-flink-$flinkMajorVersion",

        libraryDependencies ++= Seq(
          "org.apache.flink" % "flink-runtime" % flinkVersion % "provided",
          Dependencies.log4jSlf4jImpl % "test",
          Dependencies.log4j12Api % "test"
        ) ++ commonUnitTestDependencies
      )
  }

  def flinkIt: Project = {
    Project("celeborn-flink-it", file("tests/flink-it"))
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(CelebornCommon.common % "test->test;compile->compile")
      .dependsOn(CelebornClient.client % "test->test;compile->compile")
      .dependsOn(CelebornMaster.master % "test->test;compile->compile")
      .dependsOn(CelebornWorker.worker % "test->test;compile->compile")
      .dependsOn(flinkClient % "test->test;compile->compile")
      .settings (
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.flink" % "flink-runtime" % flinkVersion % "test",
          "org.apache.flink" %% "flink-scala" % flinkVersion % "test",
          flinkStreamingDependency,
          flinkClientsDependency,
          flinkRuntimeWebDependency
        ) ++ commonUnitTestDependencies
      )
  }

  def flinkClientShade: Project = {
    Project(flinkClientShadedProjectName, file(flinkClientShadedProjectPath))
      .dependsOn(flinkClient)
      .disablePlugins(AddMetaInfLicenseFiles)
      .settings (
        commonSettings,
        releaseSettings,

        moduleName := s"celeborn-client-flink-$flinkMajorVersion-shaded",

        (assembly / test) := { },

        (assembly / assemblyJarName) := {
            val revision: String = version.value
            val artifactValue: Artifact = artifact.value
            flinkClientShadeJarName(revision, artifactValue, scalaBinaryVersion.value)
        },

        (assembly / logLevel) := Level.Info,

        // Exclude `scala-library` from assembly.
        (assembly / assemblyPackageScala / assembleArtifact) := false,

        (assembly / assemblyExcludedJars) := {
          val cp = (assembly / fullClasspath).value
          cp filter { v =>
            val name = v.data.getName
            !(name.startsWith("celeborn-") ||
                name.startsWith("protobuf-java-") ||
                name.startsWith("guava-") ||
                name.startsWith("failureaccess-") ||
                name.startsWith("netty-") ||
                name.startsWith("commons-lang3-") ||
                name.startsWith("RoaringBitmap-"))
          }
        },

        (assembly / assemblyShadeRules) := Seq(
          ShadeRule.rename("com.google.protobuf.**" -> "org.apache.celeborn.shaded.com.google.protobuf.@1").inAll,
          ShadeRule.rename("com.google.common.**" -> "org.apache.celeborn.shaded.com.google.common.@1").inAll,
          ShadeRule.rename("io.netty.**" -> "org.apache.celeborn.shaded.io.netty.@1").inAll,
          ShadeRule.rename("org.apache.commons.**" -> "org.apache.celeborn.shaded.org.apache.commons.@1").inAll,
          ShadeRule.rename("org.roaringbitmap.**" -> "org.apache.celeborn.shaded.org.roaringbitmap.@1").inAll
        ),

        (assembly / assemblyMergeStrategy) := {
          case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
          // the LicenseAndNoticeMergeStrategy always picks the license/notice file from the current project
          case m @ ("META-INF/LICENSE" | "META-INF/NOTICE") => CustomMergeStrategy("LicenseAndNoticeMergeStrategy") { conflicts =>
            val entry = conflicts.head
            val projectLicenseFile = (Compile / resourceDirectory).value / entry.target
            val stream = () => new java.io.BufferedInputStream(new java.io.FileInputStream(projectLicenseFile))
            Right(Vector(JarEntry(entry.target, stream)))
          }
          case PathList(ps@_*) if Assembly.isLicenseFile(ps.last) => MergeStrategy.discard
          // Drop all proto files that are not needed as artifacts of the build.
          case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
          case m if m.toLowerCase(Locale.ROOT).startsWith("meta-inf/native-image") => MergeStrategy.discard
          // Drop netty jnilib
          case m if m.toLowerCase(Locale.ROOT).endsWith(".jnilib") => MergeStrategy.discard
          // rename netty native lib
          case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => CustomMergeStrategy.rename( _ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_x86_64.so" )
          case "META-INF/native/libnetty_transport_native_epoll_aarch_64.so" => CustomMergeStrategy.rename( _ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_aarch_64.so" )
          case _ => MergeStrategy.first
        },

        Compile / packageBin := assembly.value,
        pomPostProcess := removeDependenciesTransformer
      )
  }
}

////////////////////////////////////////////////////////
//                   MR Client                        //
////////////////////////////////////////////////////////
object MRClientProjects {

  def mrClient: Project = {
    Project("celeborn-client-mr", file("client-mr/mr"))
      .dependsOn(CelebornCommon.common, CelebornClient.client)
      .settings(
        commonSettings,
        libraryDependencies ++= Seq(
          Dependencies.hadoopClientApi,
          Dependencies.hadoopClientRuntime,
          Dependencies.hadoopMapreduceClientApp,
          Dependencies.jacksonJaxrsJsonProvider,
          Dependencies.jakartaActivationApi
        ) ++ commonUnitTestDependencies,
        dependencyOverrides += Dependencies.commonsCompress
      )
  }

  def mrIt: Project = {
    Project("celeborn-mr-it", file("tests/mr-it"))
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(CelebornCommon.common % "test->test;compile->compile")
      .dependsOn(CelebornClient.client % "test->test;compile->compile")
      .dependsOn(CelebornMaster.master % "test->test;compile->compile")
      .dependsOn(CelebornWorker.worker % "test->test;compile->compile")
      .dependsOn(mrClient % "test->test;compile->compile")
      .settings(
        commonSettings,
        copyDepsSettings,
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-client-minicluster" % Dependencies.hadoopVersion % "test",
          "org.apache.hadoop" % "hadoop-mapreduce-examples" % Dependencies.hadoopVersion % "test",
          "org.bouncycastle" % "bcpkix-jdk15on" % "1.68" % "test"
        ) ++ commonUnitTestDependencies
      )
  }

  def mrClientShade: Project = {
    Project("celeborn-client-mr-shaded", file("client-mr/mr-shaded"))
      .dependsOn(mrClient)
      .disablePlugins(AddMetaInfLicenseFiles)
      .settings(
        commonSettings,
        releaseSettings,

        // align final shaded jar name with maven.
        (assembly / assemblyJarName) := {
          val extension = artifact.value.extension
          s"${moduleName.value}_${scalaBinaryVersion.value}-${version.value}.$extension"
        },

        (assembly / test) := {},

        (assembly / logLevel) := Level.Info,

        // include `scala-library` from assembly.
        (assembly / assemblyPackageScala / assembleArtifact) := true,

        (assembly / assemblyExcludedJars) := {
          val cp = (assembly / fullClasspath).value
          cp filter { v =>
            val name = v.data.getName
            !(name.startsWith("celeborn-") ||
              name.startsWith("protobuf-java-") ||
              name.startsWith("guava-") ||
              name.startsWith("failureaccess-") ||
              name.startsWith("netty-") ||
              name.startsWith("commons-lang3-") ||
              name.startsWith("RoaringBitmap-") ||
              name.startsWith("lz4-java-") ||
              name.startsWith("zstd-jni-") ||
              name.startsWith("scala-library-"))
          }
        },

        (assembly / assemblyShadeRules) := Seq(
          ShadeRule.rename("com.google.protobuf.**" -> "org.apache.celeborn.shaded.com.google.protobuf.@1").inAll,
          ShadeRule.rename("com.google.common.**" -> "org.apache.celeborn.shaded.com.google.common.@1").inAll,
          ShadeRule.rename("io.netty.**" -> "org.apache.celeborn.shaded.io.netty.@1").inAll,
          ShadeRule.rename("org.apache.commons.**" -> "org.apache.celeborn.shaded.org.apache.commons.@1").inAll,
          ShadeRule.rename("org.roaringbitmap.**" -> "org.apache.celeborn.shaded.org.roaringbitmap.@1").inAll
        ),

        (assembly / assemblyMergeStrategy) := {
          case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
          // For netty-3.x.y.Final.jar
          case m if m.startsWith("META-INF/license/") => MergeStrategy.discard
          // the LicenseAndNoticeMergeStrategy always picks the license/notice file from the current project
          case m @ ("META-INF/LICENSE" | "META-INF/NOTICE") => CustomMergeStrategy("LicenseAndNoticeMergeStrategy") { conflicts =>
            val entry = conflicts.head
            val projectLicenseFile = (Compile / resourceDirectory).value / entry.target
            val stream = () => new java.io.BufferedInputStream(new java.io.FileInputStream(projectLicenseFile))
            Right(Vector(JarEntry(entry.target, stream)))
          }
          case PathList(ps@_*) if Assembly.isLicenseFile(ps.last) => MergeStrategy.discard
          // Drop all proto files that are not needed as artifacts of the build.
          case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
          case m if m.toLowerCase(Locale.ROOT).startsWith("meta-inf/native-image") => MergeStrategy.discard
          // Drop netty jnilib
          case m if m.toLowerCase(Locale.ROOT).endsWith(".jnilib") => MergeStrategy.discard
          // rename netty native lib
          case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => CustomMergeStrategy.rename(_ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_x86_64.so")
          case "META-INF/native/libnetty_transport_native_epoll_aarch_64.so" => CustomMergeStrategy.rename(_ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_aarch_64.so")
          case _ => MergeStrategy.first
        },

        Compile / packageBin := assembly.value,
        pomPostProcess := removeDependenciesTransformer
      )
  }

  def modules: Seq[Project] = {
    Seq(mrClient, mrIt, mrGroup, mrClientShade)
  }

  // for test only, don't use this group for any other projects
  lazy val mrGroup = (project withId "celeborn-mr-group").aggregate(mrClient, mrIt)

  val copyDeps = TaskKey[Unit]("copyDeps", "Copies needed dependencies to the build directory.")
  val destPath = (Compile / crossTarget) {
    _ / "mapreduce_lib"
  }

  lazy val copyDepsSettings = Seq(
    copyDeps := {
      val dest = destPath.value
      if (!dest.isDirectory() && !dest.mkdirs()) {
        throw new java.io.IOException("Failed to create jars directory.")
      }

      (Compile / dependencyClasspath).value.map(_.data)
        .filter { jar => jar.isFile() }
        .foreach { jar =>
          val destJar = new File(dest, jar.getName())
          if (destJar.isFile()) {
            destJar.delete()
          }
          Files.copy(jar.toPath(), destJar.toPath())
        }
    },
    (Test / compile) := {
      copyDeps.value
      (Test / compile).value
    }
  )
}

object CelebornOpenApi {
  val openApiSpecDir = "openapi/openapi-client/src/main/openapi3"
  val openApiMasterInternalOutputDir = "openapi/openapi-client/target/master/generated-sources/java"
  val openApiWorkerInternalOutputDir = "openapi/openapi-client/target/worker/generated-sources/java"
  val openApiClientOutputDir = "openapi/openapi-client/src/main/java"

  val generate = TaskKey[Unit]("generate", "generate openapi client code")

  val commonOpenApiClientGenerateSettings = Seq(
    openApiGeneratorName := "java",
    openApiGenerateApiTests := SettingDisabled,
    openApiGenerateModelTests := SettingDisabled,
    openApiModelPackage := "org.apache.celeborn.rest.v1.model",
    openApiAdditionalProperties := Map(
      "dateLibrary" -> "java8",
      "useGzipFeature" -> "true",
      "library" -> "apache-httpclient",
      "hideGenerationTimestamp" -> "true",
      "supportUrlQuery" -> "false",
      "annotationLibrary" -> "none",
      "templateDir" -> s"$openApiSpecDir/templates",
      "useEnumCaseInsensitive" -> "true"
    )
  )

  lazy val openApiClientMasterGenerate = Project("celeborn-openapi-client-master-generate", file("openapi/openapi-client/target/master"))
    .enablePlugins(OpenApiGeneratorPlugin)
    .settings(
      commonSettings,
      openApiInputSpec := (file(openApiSpecDir) / "master_rest_v1.yaml").toString,
      openApiOutputDir := openApiMasterInternalOutputDir,
      openApiApiPackage := "org.apache.celeborn.rest.v1.master",
      openApiInvokerPackage := "org.apache.celeborn.rest.v1.master.invoker",
      commonOpenApiClientGenerateSettings
    )

  lazy val openApiClientWorkerGenerate = Project("celeborn-openapi-client-worker-generate", file("openapi/openapi-client/target/worker"))
    .enablePlugins(OpenApiGeneratorPlugin)
    .settings(
      commonSettings,
      openApiInputSpec := (file(openApiSpecDir) / "worker_rest_v1.yaml").toString,
      openApiOutputDir := openApiWorkerInternalOutputDir,
      openApiApiPackage := "org.apache.celeborn.rest.v1.worker",
      openApiInvokerPackage := "org.apache.celeborn.rest.v1.worker.invoker",
      commonOpenApiClientGenerateSettings
    )

  lazy val openApiClient = Project("celeborn-openapi-client", file("openapi/openapi-client"))
    .settings (
      commonSettings,
      releaseSettings,
      libraryDependencies ++= Seq(
        Dependencies.jacksonAnnotations,
        Dependencies.jacksonCore,
        Dependencies.jacksonDatabind,
        Dependencies.jacksonDataTypeJsr310,
        Dependencies.jacksonJaxrsJsonProvider,
        Dependencies.findbugsJsr305,
        Dependencies.jakartaAnnotationApi,
        Dependencies.httpClient5,
        Dependencies.httpCore5,
        Dependencies.httpCore5H2,
        Dependencies.openApiToolsJacksonBindNullable,
        Dependencies.slf4jApi
      ),

      generate := {
        (openApiClientMasterGenerate / Compile / openApiGenerate).value
        (openApiClientWorkerGenerate / Compile / openApiGenerate).value

        streams.value.log.info("Cleaning up openapi generate output directory: " + openApiClientOutputDir)
        val dstDir = file(openApiClientOutputDir)
        IO.delete(dstDir)

        val masterSrcDir = file(openApiMasterInternalOutputDir) / "src" / "main" / "java"
        streams.value.log.info(s"Copying openapi generated master sources from $masterSrcDir to $dstDir")
        IO.copyDirectory(masterSrcDir, dstDir)

        val workerSrcDir = file(openApiWorkerInternalOutputDir) / "src" / "main" / "java"
        streams.value.log.info(s"Copying openapi generated worker sources from $workerSrcDir to $dstDir")
        IO.copyDirectory(workerSrcDir, dstDir)
      },

      (assembly / test) := { },
      (assembly / assemblyJarName) := {
        s"${moduleName.value}-${version.value}.${artifact.value.extension}"
      },
      (assembly / logLevel) := Level.Info,
      // Exclude `scala-library` from assembly.
      (assembly / assemblyPackageScala / assembleArtifact) := false,
      (assembly / assemblyExcludedJars) := {
        val cp = (assembly / fullClasspath).value
        cp filter { v =>
          val name = v.data.getName
          !(name.startsWith("celeborn-") ||
            name.startsWith("jackson-annotations-") ||
            name.startsWith("jackson-core-") ||
            name.startsWith("jackson-databind-") ||
            name.startsWith("jackson-datatype-jsr310-") ||
            name.startsWith("jackson-jaxrs-json-provider-") ||
            name.startsWith("jsr305-") ||
            name.startsWith("jakarta.annotation-api-") ||
            name.startsWith("httpclient5-") ||
            name.startsWith("httpcore5-") ||
            name.startsWith("httpcore5-h2-") ||
            name.startsWith("jackson-databind-nullable-") ||
            name.startsWith("slf4j-api-"))
        }
      },

      (assembly / assemblyShadeRules) := Seq(
        ShadeRule.rename("org.openapitools.**" -> "org.apache.celeborn.shaded.org.openapitools.@1").inAll,
        ShadeRule.rename("javax.annotation.**" -> "org.apache.celeborn.shaded.javax.annotation.@1").inAll,
        ShadeRule.rename("com.fasterxml.jackson.**" -> "org.apache.celeborn.shaded.com.fasterxml.jackson.@1").inAll,
        ShadeRule.rename("jakarta.validation.**" -> "org.apache.celeborn.shaded.jakarta.validation.@1").inAll,
        ShadeRule.rename("javax.validation.**" -> "org.apache.celeborn.shaded.javax.validation.@1").inAll,
        ShadeRule.rename("javax.ws.rs.ext.**" -> "org.apache.celeborn.shaded.javax.ws.rs.ext.@1").inAll,
        ShadeRule.rename("org.apache.hc.**" -> "org.apache.celeborn.shaded.org.apache.hc.@1").inAll,
        ShadeRule.rename("org.slf4j.**" -> "org.apache.celeborn.shaded.org.slf4j.@1").inAll
    ),

    (assembly / assemblyMergeStrategy) := {
      case m if m.toLowerCase(Locale.ROOT).endsWith("license") => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).endsWith("meta-inf/dependencies") => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).endsWith("module-info.class") => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).endsWith("mozilla/public-suffix-list.txt") => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).endsWith("notice") => MergeStrategy.discard
      case PathList(ps@_*) if Assembly.isLicenseFile(ps.last) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    Compile / packageBin := assembly.value,
    pomPostProcess := removeDependenciesTransformer,
    Compile / doc := {
      // skip due to doc generation failure for openapi modules, see CELEBORN-1477
      target.value / "none"
    }
  )
}

object WebProjects {

  def web: Project = {
    Project("celeborn-web", file("web"))
      .settings(commonSettings)
  }

  def modules: Seq[Project] = {
    Seq(web)
  }
}
