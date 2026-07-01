plugins {
    alias(libs.plugins.kotlinJvm)
}

kotlin {
    jvmToolchain(17)
}

dependencies {
    implementation(projects.api)
    implementation(projects.server)

    implementation(libs.kotlin.inject.runtime)
    implementation(libs.ktbuf.library)
    implementation(libs.ktbuf.rpc)
    implementation(libs.ktbuf.server)
    implementation(libs.ktbuf.test)
    implementation(libs.ktstore.library)
    implementation(libs.coroutines.core)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.cio)
    implementation(libs.ktor.server.websockets)
}
