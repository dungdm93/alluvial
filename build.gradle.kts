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
}

val slf4jVersion = "1.7.36"
val logbackVersion = "1.2.10"

val junitVersion = "5.8.2"
val striktVersion = "0.33.0"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.0")
    api("org.slf4j:slf4j-api:$slf4jVersion")
    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")

    // Test Frameworks
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.junit.vintage:junit-vintage-engine:$junitVersion")
    testImplementation("io.strikt:strikt-jvm:$striktVersion")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
