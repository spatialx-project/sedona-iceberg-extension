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
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

dependencies {
    implementation("org.apache.sedona:sedona-spark-shaded-3.0_2.12")
}

tasks {
    shadowJar {
        archiveClassifier.set("")

        // Excluded classes will be provided by sedona-iceberg-extension.
        exclude("**/GeometryUDT*.class")
        exclude("**/UdtRegistratorWrapper*.class")
        exclude("**/utils/GeometrySerializer*.class")

        // Remove bundled jackson, use jackson provided by spark runtime.
        exclude("**/com/fasterxml/jackson/**")
        exclude("**/com.fasterxml.jackson*/**")

        // Replace GeometryUDT defined by sedona with GeometryUDT defined by iceberg-spark, so that
        // we'll only have one single definition of GeometryUDT. This is the most important change
        // for seamless integration with sedona.
        relocate("org.apache.spark.sql.sedona_sql.UDT", "org.apache.spark.sql.iceberg.udt") {
            include("org.apache.spark.sql.sedona_sql.UDT.GeometryUDT*")
        }
    }
    jar {
        dependsOn(shadowJar)
        archiveClassifier.set("empty")
    }
}
