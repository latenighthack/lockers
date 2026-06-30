plugins {
    alias(libs.plugins.kotlinJvm)
    application
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("com.latenighthack.kitkit.server.MainKt")
}

dependencies {
    implementation(projects.server)
    implementation(libs.kotlin.inject.runtime)
    implementation(libs.coroutines.core)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.cio)
    implementation(libs.ktor.server.websockets)
}
