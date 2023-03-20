rootProject.name = "sedona-iceberg-extension"
include("patched-sedona", "iceberg-spark-runtime", "extension", "example")

val sparkVersion =
    if (System.getProperty("sparkVersion") != null) System.getProperty("sparkVersion")
    else System.getProperty("defaultSparkVersion")

project(":patched-sedona").name = "patched-sedona-3.0_2.12"
project(":iceberg-spark-runtime").name = "iceberg-spark-runtime-${sparkVersion}_2.12"
project(":extension").name = "sedona-iceberg-extension-${sparkVersion}_2.12"
project(":example").name = "sedona-iceberg-example-${sparkVersion}_2.12"
