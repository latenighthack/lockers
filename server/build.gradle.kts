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
    api(projects.observabilityApi)
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
    api("io.opentelemetry:opentelemetry-api:1.45.0")
    implementation("io.opentelemetry:opentelemetry-extension-kotlin:1.45.0")
    implementation(libs.cache4k)
    // Publish the aligned Netty platform to consumers; Pushy/Firebase use Netty clients.
    implementation(platform(libs.netty.bom))
    // The unshaded Netty BOM cannot update gRPC's relocated transport.
    implementation(platform(libs.grpc.bom))
    // Own the library runtime floor too, rather than relying on :server:run to override it.
    runtimeOnly(libs.postgresql)
    implementation(libs.pushy)
    implementation(libs.firebase.admin)
    implementation(libs.webpush)

    ksp(libs.kotlin.inject.ksp)

    testImplementation(kotlin("test"))
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing:1.45.0")
    testImplementation(projects.server.test)
    testImplementation(projects.connector)
    testImplementation(libs.coroutines.test)
    testImplementation(libs.ktbuf.test)
    testImplementation(libs.assertk)
    testImplementation(libs.grpc.netty.shaded)
    testImplementation(libs.grpc.stub)
    testImplementation(libs.sqlite.jdbc)
    // Real-Postgres claim-store tests (gated on LOCKERS_TEST_PG_URL; see PgTestGate).
    testRuntimeOnly(libs.postgresql)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

// Inspect resolved artifacts; catalog requests alone do not establish a patched graph.
val verifyRuntimeSecurity by tasks.registering {
    group = "verification"
    description = "Verify patched JDBC and consistent Netty artifacts in the library runtime"
    val runtime = configurations.named("runtimeClasspath")
    inputs.files(runtime)
    doLast {
        val artifacts = runtime.get().resolvedConfiguration.resolvedArtifacts
        val jdbc = artifacts.filter { it.moduleVersion.id.group == "org.postgresql" }
        check(jdbc.single().moduleVersion.id.version == libs.versions.postgresql.get()) {
            "Runtime must select the reviewed pgJDBC security release"
        }
        val grpc = artifacts.filter { it.moduleVersion.id.group == "io.grpc" }
        check(grpc.isNotEmpty() && grpc.all { it.moduleVersion.id.version == libs.versions.grpc.get() }) {
            "Runtime gRPC modules must include the reviewed relocated transport fixes"
        }
        val netty = artifacts.filter { it.moduleVersion.id.group == "io.netty" && it.name != "netty-tcnative-boringssl-static" && it.name != "netty-tcnative-classes" }
        check(netty.isNotEmpty() && netty.all { it.moduleVersion.id.version == libs.versions.netty.get() }) {
            "Runtime Netty modules must match the reviewed BOM release"
        }
    }
}
tasks.named("check") { dependsOn(verifyRuntimeSecurity) }
