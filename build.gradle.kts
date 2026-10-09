plugins {
    alias(libs.plugins.kotlinMultiplatform) apply false
    alias(libs.plugins.kotlinJvm) apply false
    alias(libs.plugins.kotlinSerialization) apply false
    alias(libs.plugins.ksp) apply false
    alias(libs.plugins.androidLibrary) apply false
    alias(libs.plugins.detekt) apply false
    alias(libs.plugins.mavenPublish) apply false
}

allprojects {
    group = "com.latenighthack.lockers"
    // Version is release-vs-SNAPSHOT driven by the CI ref:
    //   v* tag  -> the tag value (release, e.g. 0.1.0)
    //   otherwise (main push / local) -> "<baseVersion>-SNAPSHOT"
    // Bump `baseVersion` in gradle.properties after cutting a release.
    val base = providers.gradleProperty("baseVersion").get()
    val ref = System.getenv("GITHUB_REF").orEmpty()
    version = if (ref.startsWith("refs/tags/v")) {
        System.getenv("GITHUB_REF_NAME").removePrefix("v")
    } else {
        "$base-SNAPSHOT"
    }
}

subprojects {
    apply(plugin = "io.gitlab.arturbosch.detekt")
    configure<io.gitlab.arturbosch.detekt.extensions.DetektExtension> {
        buildUponDefaultConfig = true
        ignoreFailures = false
        config.setFrom(rootProject.files("config/detekt/config.yml"))
        baseline = rootProject.file("config/detekt/${project.path.trimStart(':').replace(':', '-')}.xml")
        // Include handwritten Kotlin from every platform; generated build bindings
        // and migration fixtures are compiler outputs, not handwritten source.
        source.setFrom(fileTree("src") { include("**/*.kt"); exclude("**/generated/**") })
        basePath = rootProject.projectDir.path
    }
}
