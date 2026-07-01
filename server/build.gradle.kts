plugins {
    alias(libs.plugins.kotlinJvm)
    alias(libs.plugins.ksp)
}

kotlin {
    jvmToolchain(17)
}

dependencies {
    implementation(projects.api)

    implementation(libs.kotlin.inject.runtime)
    implementation(libs.ktbuf.library)
    implementation(libs.ktbuf.rpc)
    implementation(libs.ktbuf.server)
    implementation(libs.coroutines.core)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.cio)
    implementation(libs.ktor.server.websockets)

    implementation(libs.ktstore.library)
    implementation(libs.ktcrypto.library)
    implementation(libs.kotlinx.datetime)
    implementation(libs.micrometer.core)
    implementation(libs.cache4k)
    implementation(libs.pushy)

    ksp(libs.kotlin.inject.ksp)

    testImplementation(kotlin("test"))
    testImplementation(libs.coroutines.test)
    testImplementation(libs.ktbuf.test)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
