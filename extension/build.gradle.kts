/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

plugins {
    id("org.apache.iceberg.sedona.common")
    scala
    `java-library`
    `maven-publish`
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

configurations {
    configurations.testImplementation.get().apply {
        extendsFrom(configurations.compileOnly.get())
    }
}

repositories {
    mavenLocal()
    maven {
        url = uri("https://maven.pkg.github.com/spatialx-project/geolake-iceberg")
        credentials {
            username = project.findProperty("gpr.user") as String? ?: System.getenv("USERNAME")
            password = project.findProperty("gpr.key") as String? ?: System.getenv("TOKEN")
        }
    }
}

val sparkVersion =
    if (System.getProperty("sparkVersion") != null) System.getProperty("sparkVersion")
    else System.getProperty("defaultSparkVersion")

dependencies {
    implementation(project(":patched-sedona-3.0_2.12", "shadow")) {
        exclude("org.apache.sedona")
    }
    compileOnly("org.apache.iceberg:iceberg-spark-${sparkVersion}_2.12")
    compileOnly("org.apache.iceberg:iceberg-spark-extensions-${sparkVersion}_2.12")
    compileOnly("org.apache.spark:spark-sql_2.12")

    testImplementation("org.apache.iceberg:iceberg-hive-metastore")
    testImplementation("org.datasyslab:geotools-wrapper")
    testImplementation("org.apache.spark:spark-hive_2.12:${sparkVersion}.0") {
        exclude("org.apache.avro", "avro")
        exclude("org.apache.arrow")
        exclude("org.apache.parquet")
        // to make sure io.netty.buffer only comes from project(':iceberg-arrow')
        exclude("io.netty", "netty-buffer")
        exclude("org.roaringbitmap")
    }
}

tasks {
    shadowJar {
        archiveClassifier.set("")
    }

    jar {
        dependsOn(shadowJar)
        archiveClassifier.set("original")
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "org.apache.iceberg"
            versionMapping {
                allVariants {
                    fromResolutionResult()
                }
            }
            pom {
                name.set("Sedona-Iceberg Extension")
                description.set("Unleash the power of Apache Sedona when processing Iceberg Tables")
                url.set("https://iceberg.apache.org")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
            }

            repositories {
                maven {
                    name = "GitHubPackages"
                    url = uri("https://maven.pkg.github.com/spatialx-project/sedona-iceberg-extension")
                    credentials {
                        username = project.findProperty("gpr.user") as String? ?: System.getenv("USERNAME")
                        password = project.findProperty("gpr.key") as String? ?: System.getenv("TOKEN")
                    }
                }
            }

            from(components["java"])
        }
    }
}
