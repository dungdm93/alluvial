plugins {
    application
    java
    kotlin("jvm") version "1.6.10"
    id("io.spring.dependency-management") version "1.0.11.RELEASE"
}

group = "dev.alluvial"
version = "0.1"

java.toolchain {
    languageVersion.set(JavaLanguageVersion.of(11))
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
val logbackVersion = "1.2.10"
val debeziumVersion = "1.9.1.Final"
val kafkaVersion = "3.0.0"
val confluentVersion = "7.0.0"
val icebergVersion = "0.13.1"
val hadoopVersion = "3.2.1"
val hiveVersion = "3.1.3"
val jacksonVersion = "2.12.3"

val junitVersion = "5.8.2"
val striktVersion = "0.33.0"
val assertjVersion = "3.22.0"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.0")
    api("org.slf4j:slf4j-api:$slf4jVersion")
    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")
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
    // Iceberg transient dependency
    implementation("org.apache.iceberg:iceberg-common:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-bundled-guava:$icebergVersion")
    implementation("com.github.ben-manes.caffeine:caffeine:2.8.4")
    implementation("com.github.stephenc.findbugs:findbugs-annotations:1.3.9-1")
    implementation("org.roaringbitmap:RoaringBitmap:0.9.22")
    implementation("org.apache.avro:avro:1.10.1")
    implementation("org.apache.orc:orc-core:1.7.2")
    implementation("org.apache.parquet:parquet-avro:1.12.2")

    // Test Frameworks
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
    testImplementation("org.junit.vintage:junit-vintage-engine:$junitVersion")
    testImplementation("org.assertj:assertj-core:$assertjVersion")
    testImplementation("io.strikt:strikt-jvm:$striktVersion")
    // Test deps
    testImplementation("org.apache.iceberg:iceberg-api:$icebergVersion:tests")
    testImplementation("org.apache.iceberg:iceberg-core:$icebergVersion:tests")
    testImplementation("org.apache.iceberg:iceberg-data:$icebergVersion:tests")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
