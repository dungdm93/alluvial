plugins {
    java
    kotlin("jvm") version "1.9.0"
    application
}

group = "dev.alluvial"
version = "0.1"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

kotlin {
    jvmToolchain(17)
}

application {
    applicationName = "alluvial"
    mainClass.set("dev.alluvial.runtime.Main")
}

repositories {
    mavenCentral()
    maven {
        name = "confluent"
        url = uri("https://packages.confluent.io/maven/")
    }
    maven {
        name = "jitpack"
        url = uri("https://jitpack.io")
    }
}

val slf4jVersion = "1.7.36"
val logbackVersion = "1.2.12"   // pinned. logback 1.3+ requires slf4j 2.x
val debeziumVersion = "2.3.2.Final"
val kafkaVersion = "3.5.1"
val confluentVersion = "7.4.0"
val icebergVersion = "1.3.1"
val hadoopVersion = "3.3.2"
val hiveVersion = "3.1.3"
val awsVersion = "2.19.4"
val jacksonVersion = "2.14.2"
val micrometerVersion = "1.9.0"
val prometheusVersion = "0.15.0"

val junitVersion = "5.9.0"
val striktVersion = "0.34.1"
val assertjVersion = "3.23.1"
val mockkVersion = "1.13.2"

dependencies {
    implementation(kotlin("stdlib"))
    api("org.slf4j:slf4j-api:$slf4jVersion")
    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("io.micrometer:micrometer-core:$micrometerVersion")
    implementation("io.micrometer:micrometer-registry-prometheus:$micrometerVersion")
    implementation("io.prometheus:simpleclient_httpserver:$prometheusVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")

    // Debezium
    implementation("io.debezium:debezium-ddl-parser:$debeziumVersion")

    // Kafka
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("org.apache.kafka:connect-api:$kafkaVersion")
    runtimeOnly("org.apache.kafka:connect-json:$kafkaVersion")
    runtimeOnly("io.confluent:kafka-connect-json-schema-converter:$confluentVersion")
    runtimeOnly("io.confluent:kafka-connect-protobuf-converter:$confluentVersion")
    runtimeOnly("io.confluent:kafka-connect-avro-converter:$confluentVersion")

    // Iceberg
    implementation("org.apache.iceberg:iceberg-core:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-data:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-orc:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-parquet:$icebergVersion")
    implementation("org.apache.hadoop:hadoop-client:$hadoopVersion") {
        exclude(group = "log4j")
        exclude(group = "org.slf4j")
        exclude(group = "commons-logging")
        exclude(group = "commons-beanutils")
        exclude(group = "org.apache.avro")
        exclude(group = "javax.servlet")
        exclude(group = "com.google.code.gson")
    }
    runtimeOnly("org.apache.iceberg:iceberg-hive-metastore:$icebergVersion")
    // "org.apache.hive:hive-standalone-metastore:$hiveVersion"
    // "org.apache.hive:hive-common:$hiveVersion"
    // "org.apache.hive:hive-serde:$hiveVersion"
    runtimeOnly("org.apache.hive:hive-metastore:$hiveVersion") {
        exclude(group = "log4j")
        exclude(group = "org.slf4j")
        exclude(group = "org.apache.logging.log4j")
        exclude(group = "org.apache.avro")
        exclude(group = "org.apache.orc")
        exclude(group = "org.apache.parquet")
        exclude(group = "net.sf.opencsv")
        exclude(group = "org.apache.ant")
        exclude(group = "org.apache.arrow")
        exclude(group = "org.apache.hadoop")
        exclude(group = "org.apache.hbase")
        exclude(group = "org.apache.twill")
        exclude(group = "org.apache.derby")
        exclude(group = "org.apache.curator")
        exclude(group = "org.apache.zookeeper")
        exclude(group = "co.cask.tephra") // org.apache.tephra
        exclude(group = "javax.servlet")
        exclude(group = "javax.xml.bind")
        exclude(group = "javax.jdo")
        exclude(group = "javax.transaction")
        exclude(group = "org.datanucleus")
        exclude(group = "com.zaxxer", module = "HikariCP")
        exclude(group = "com.jolbox", module = "bonecp")
        exclude(group = "commons-dbcp")
        exclude(group = "commons-pool")
        exclude(group = "commons-codec")
        exclude(group = "commons-logging")
        exclude(group = "commons-cli")
        exclude(group = "jline")
        exclude(group = "sqlline")
        exclude(group = "javolution")
        exclude(group = "org.antlr")
        exclude(group = "joda-time")
        exclude(group = "net.sf.jpam")
        exclude(group = "com.sun.jersey")
        exclude(group = "org.eclipse.jetty")
        exclude(group = "io.dropwizard.metrics")
        exclude(group = "com.github.joshelser", module = "dropwizard-metrics-hadoop-metrics2-reporter")
        exclude(group = "com.tdunning", module = "json")
        exclude(group = "org.codehaus.jettison", module = "jettison")
    }
    implementation("org.apache.iceberg:iceberg-aws:$icebergVersion")
    implementation("software.amazon.awssdk:s3:$awsVersion")
    implementation("software.amazon.awssdk:kms:$awsVersion")
    implementation("software.amazon.awssdk:glue:$awsVersion")
    implementation("software.amazon.awssdk:sts:$awsVersion")
    implementation("software.amazon.awssdk:dynamodb:$awsVersion")

    // Make those Iceberg's transient dependencies available in compile scope
    implementation("org.apache.iceberg:iceberg-common:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-bundled-guava:$icebergVersion")
    implementation("com.google.guava:guava:31.1-jre")
    implementation("org.apache.avro:avro:1.11.1")
    implementation("org.apache.orc:orc-core:1.8.3:nohive")
    implementation("org.apache.parquet:parquet-avro:1.13.1")

    // Test Frameworks
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
    testImplementation("org.junit.vintage:junit-vintage-engine:$junitVersion")
    testImplementation("org.assertj:assertj-core:$assertjVersion")
    testImplementation("io.strikt:strikt-jvm:$striktVersion")
    testImplementation("io.mockk:mockk:$mockkVersion")

    // Test deps
    testImplementation("org.apache.iceberg:iceberg-api:$icebergVersion:tests")
    testImplementation("org.apache.iceberg:iceberg-core:$icebergVersion:tests")
    testImplementation("org.apache.iceberg:iceberg-data:$icebergVersion:tests")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
