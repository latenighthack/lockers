plugins {
    alias(libs.plugins.kotlinJvm)
    application
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("com.latenighthack.lockers.server.MainKt")
}

dependencies {
    implementation(projects.server)
    implementation(projects.observabilityServer)
    implementation("io.opentelemetry:opentelemetry-sdk-extension-autoconfigure:1.45.0")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:1.45.0")
    implementation("io.opentelemetry:opentelemetry-extension-kotlin:1.45.0")
    implementation(libs.kotlin.inject.runtime)
    implementation(libs.ktstore.library)
    implementation(libs.coroutines.core)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.cio)
    implementation(libs.ktor.server.websockets)
    implementation(libs.micrometer.core)
    implementation(libs.micrometer.registry.prometheus)

    runtimeOnly(libs.postgresql)
    runtimeOnly(libs.logback.classic)
    runtimeOnly(libs.logstash.logback.encoder)
}
