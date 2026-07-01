plugins {
    alias(libs.plugins.kotlinMultiplatform)
    `java-library`
}

kotlin {
    jvmToolchain(17)

    jvm {
        withJava()
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
