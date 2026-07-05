plugins {
    alias(libs.plugins.kotlinJvm)
    application
    alias(libs.plugins.shadow)
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("com.latenighthack.lockers.keymaster.MainKt")
    applicationName = "keymaster"
}

dependencies {
    implementation(projects.api)
    implementation(libs.ktbuf.library)
    implementation(libs.ktbuf.rpc)
    implementation(libs.coroutines.core)
    implementation(libs.clikt)

    runtimeOnly(libs.logback.classic)

    testImplementation(kotlin("test"))
    testImplementation(libs.assertk)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.shadowJar {
    archiveBaseName.set("keymaster")
    archiveClassifier.set("")
    archiveVersion.set("")
    mergeServiceFiles()
}
