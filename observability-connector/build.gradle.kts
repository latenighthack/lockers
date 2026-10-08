plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidLibrary)
    alias(libs.plugins.kotlinSerialization)
    alias(libs.plugins.mavenPublish)
}
kotlin {
    jvmToolchain(17)
    jvm()
    androidTarget { publishLibraryVariants("release") }
    iosArm64(); iosX64(); iosSimulatorArm64()
    js(IR) { browser(); nodejs(); binaries.library() }
    sourceSets {
        commonMain.dependencies {
            api(projects.observabilityApi)
            api(libs.coroutines.core)
        }
        commonTest.dependencies { implementation(kotlin("test")); implementation(libs.coroutines.test) }
    }
}
android { namespace = "com.latenighthack.lockers.observability.connector"; compileSdk = 35; defaultConfig { minSdk = 24 } }
mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
    if (providers.gradleProperty("signingInMemoryKey").isPresent) signAllPublications()
    coordinates(artifactId = "lockers-observability-connector")
    pom { name.set("lockers-observability-connector"); description.set("Optional Lockers monitoring connector contracts and adapters.") }
}
