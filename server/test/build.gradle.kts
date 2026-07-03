plugins {
    alias(libs.plugins.kotlinJvm)
    alias(libs.plugins.mavenPublish)
}

kotlin {
    jvmToolchain(17)
}

mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
    signAllPublications()
    coordinates(artifactId = "lockers-server-test")
    pom {
        name.set("lockers-server-test")
        description.set("Test fixtures (attachTestServices) that boot the lockers monolith over an in-memory store.")
    }
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
