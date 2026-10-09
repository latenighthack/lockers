plugins { alias(libs.plugins.kotlinJvm); alias(libs.plugins.mavenPublish) }
kotlin { jvmToolchain(17) }
dependencies {
    api(projects.observabilityApi)
    api(projects.server)
    api(libs.micrometer.registry.prometheus)
    api("io.opentelemetry:opentelemetry-api:1.45.0")
    implementation("io.opentelemetry:opentelemetry-extension-kotlin:1.45.0")
    implementation(libs.ktstore.library)
    implementation("org.slf4j:slf4j-api:2.0.13")
    implementation(libs.coroutines.core)
    testImplementation(kotlin("test"))
    testImplementation(projects.observabilityConnector)
    testImplementation(projects.server.test)
    testImplementation(projects.api)
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing:1.45.0")
    testImplementation(libs.coroutines.test)
}
tasks.named<Test>("test") { useJUnitPlatform() }
mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
    if (providers.gradleProperty("signingInMemoryKey").isPresent) signAllPublications()
    coordinates(artifactId = "lockers-observability-server")
    pom { name.set("lockers-observability-server"); description.set("Optional Prometheus and OpenTelemetry adapters for embedded Lockers servers.") }
}
val monitoringBundle by tasks.registering(Zip::class) {
    group = "distribution"
    archiveBaseName.set("lockers-monitoring")
    archiveVersion.set(providers.provider { project.version.toString() })
    destinationDirectory.set(layout.buildDirectory.dir("distributions"))
    from(rootProject.file("monitoring"))
}
