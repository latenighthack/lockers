plugins {
    alias(libs.plugins.kotlinJvm)
    alias(libs.plugins.ksp)
    alias(libs.plugins.mavenPublish)
}

kotlin {
    jvmToolchain(17)
}

mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
    // Sign only when a key is configured (CI); local publishToMavenLocal has no signatory.
    if (providers.gradleProperty("signingInMemoryKey").isPresent) {
        signAllPublications()
    }
    coordinates(artifactId = "lockers-server")
    pom {
        name.set("lockers-server")
        description.set("JVM service host for the lockers sync primitive (session, room, push).")
    }
}

dependencies {
    implementation(projects.api)
    implementation(projects.shardingCore)

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
    implementation(libs.firebase.admin)
    implementation(libs.webpush)

    ksp(libs.kotlin.inject.ksp)

    testImplementation(kotlin("test"))
    testImplementation(projects.server.test)
    testImplementation(libs.coroutines.test)
    testImplementation(libs.ktbuf.test)
    testImplementation(libs.assertk)
    testImplementation(libs.sqlite.jdbc)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
