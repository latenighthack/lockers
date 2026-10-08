plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidLibrary)
    alias(libs.plugins.kotlinSerialization)
    alias(libs.plugins.mavenPublish)
}
kotlin {
    jvmToolchain(17)
    jvm()
    androidTarget { publishLibraryVariants("release") }
    iosArm64(); iosX64(); iosSimulatorArm64()
    js(IR) { browser(); nodejs(); binaries.library() }
    sourceSets {
        commonMain.dependencies {
            api(libs.serialization.json)
            api(libs.coroutines.core)
        }
        commonTest.dependencies { implementation(kotlin("test")); implementation(libs.coroutines.test) }
    }
}
android { namespace = "com.latenighthack.lockers.observability.api"; compileSdk = 35; defaultConfig { minSdk = 24 } }
mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
    if (providers.gradleProperty("signingInMemoryKey").isPresent) signAllPublications()
    coordinates(artifactId = "lockers-observability-api")
    pom { name.set("lockers-observability-api"); description.set("Optional Lockers monitoring api contracts and adapters.") }
}

val generateMonitoringVersion by tasks.registering {
    val libraryVersion = providers.provider { project.version.toString() }
    inputs.property("version", libraryVersion)
    val output = layout.buildDirectory.dir("generated/monitoring-version")
    outputs.dir(output)
    doLast {
        output.get().file("com/latenighthack/lockers/observability/LibraryVersion.kt").asFile.apply {
            parentFile.mkdirs()
            writeText("package com.latenighthack.lockers.observability\nconst val LOCKERS_LIBRARY_VERSION = \"${libraryVersion.get()}\"\n")
        }
    }
}
kotlin.sourceSets.getByName("commonMain").kotlin.srcDir(generateMonitoringVersion)
