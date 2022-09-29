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
    java
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()

    // FIXME: Temporarily use Apache snapshot repository.
    maven("https://repository.apache.org/content/repositories/snapshots")
}

java {
    // Configure the Java source and target compatibility.
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

val sparkVersion =
    if (System.getProperty("sparkVersion") != null) System.getProperty("sparkVersion")
    else System.getProperty("defaultSparkVersion")

dependencies {
    constraints {
        // Define dependency versions as constraints
        implementation("org.apache.spark:spark-sql_2.12:${sparkVersion}.0")
        implementation("org.apache.spark:spark-hive_2.12:${sparkVersion}.0")
        // FIXME: switch to a released version once 1.4.0-incubating was released
        implementation("org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.3.2-incubating-SNAPSHOT")
        implementation("org.datasyslab:geotools-wrapper:1.1.0-25.2")
        implementation("org.apache.iceberg:iceberg-spark-${sparkVersion}_2.12:1.1.0-gd-SNAPSHOT")
        implementation("org.apache.iceberg:iceberg-spark-extensions-${sparkVersion}_2.12:1.1.0-gd-SNAPSHOT")
        implementation("org.apache.iceberg:iceberg-hive-metastore:1.1.0-gd-SNAPSHOT")
    }

    // Use JUnit Jupiter for testing.
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}
