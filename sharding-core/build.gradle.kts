plugins {
    alias(libs.plugins.kotlinJvm)
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
    coordinates(artifactId = "lockers-sharding-core")
    pom {
        name.set("lockers-sharding-core")
        description.set(
            "Technology-agnostic ring-sharding core for lockers: partitioning, consistent-hash " +
                "assignment, epoch'd shard maps, and pluggable membership/ownership seams."
        )
    }
}

dependencies {
    implementation(libs.coroutines.core)

    testImplementation(kotlin("test"))
    testImplementation(libs.coroutines.test)
    testImplementation(libs.assertk)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
