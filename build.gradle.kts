plugins {
    application
    java
    kotlin("jvm") version "1.6.10"
    id("io.spring.dependency-management") version "1.0.11.RELEASE"
}

group = "dev.alluvial"
version = "0.1-SNAPSHOT"

java.toolchain {
    languageVersion.set(JavaLanguageVersion.of(11))
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
val kafkaVersion = "3.0.0"
val confluentVersion = "7.0.0"
val icebergVersion = "0.13.1"
val hadoopVersion = "3.2.1"

val junitVersion = "5.8.2"
val striktVersion = "0.33.0"
val assertjVersion = "3.22.0"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.0")
    api("org.slf4j:slf4j-api:$slf4jVersion")
    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")

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
    implementation("org.apache.hadoop:hadoop-common:$hadoopVersion") {
        exclude(group = "log4j")
        exclude(group = "org.slf4j")
        exclude(group = "commons-beanutils")
        exclude(group = "org.apache.avro")
        exclude(group = "javax.servlet")
        exclude(group = "com.google.code.gson")
    }
    // Iceberg transient dependency
    implementation("org.apache.iceberg:iceberg-common:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-bundled-guava:$icebergVersion")
    implementation("com.fasterxml.jackson.core:jackson-core:2.11.4")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.11.4")
    implementation("com.github.ben-manes.caffeine:caffeine:2.8.4")
    implementation("com.github.stephenc.findbugs:findbugs-annotations:1.3.9-1")
    implementation("org.roaringbitmap:RoaringBitmap:0.9.22")
    implementation("org.apache.avro:avro:1.10.1")
    implementation("org.apache.parquet:parquet-avro:1.12.2")
    implementation("org.apache.orc:orc-core:1.7.2")

    // Test Frameworks
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
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
