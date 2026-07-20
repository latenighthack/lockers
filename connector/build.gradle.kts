plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidLibrary)
    alias(libs.plugins.mavenPublish)
}

mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
    // Sign only when a key is configured (CI); local publishToMavenLocal has no signatory.
    if (providers.gradleProperty("signingInMemoryKey").isPresent) {
        signAllPublications()
    }
    coordinates(artifactId = "lockers-connector")
    pom {
        name.set("lockers-connector")
        description.set("Kotlin Multiplatform client connector for the lockers sync service.")
    }
}

kotlin {
    jvmToolchain(17)

    jvm()
    androidTarget { publishLibraryVariants("release") }
    iosArm64()
    iosX64()
    iosSimulatorArm64()
    js(IR) {
        browser()
        nodejs()
        binaries.library()
    }

    sourceSets {
        val commonMain by getting {
            dependencies {
                implementation(projects.api)
                implementation(libs.kotlin.stdlib)
                implementation(libs.ktbuf.library)
                implementation(libs.ktbuf.rpc)
                implementation(libs.ktstore.library)
                implementation(libs.ktcrypto.library)
                implementation(libs.kotlinx.datetime)
                implementation(libs.coroutines.core)
                api(libs.kmLogger)
            }
        }
        val jvmTest by getting {
            dependencies {
                implementation(kotlin("test"))
                implementation(libs.coroutines.test)
                implementation(libs.assertk)
                implementation(libs.ktbuf.server)
                implementation(libs.ktbuf.test)
                implementation(libs.ktstore.library)
                implementation(libs.ktor.server.core)
                implementation(libs.ktor.server.cio)
                implementation(libs.ktor.server.websockets)
                implementation(projects.server)
                implementation(projects.server.test)
            }
        }
    }
}

android {
    namespace = "com.latenighthack.lockers.connector"
    compileSdk = 35
    defaultConfig {
        minSdk = 24
    }
}
