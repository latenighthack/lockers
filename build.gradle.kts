plugins {
    alias(libs.plugins.kotlinMultiplatform) apply false
    alias(libs.plugins.kotlinJvm) apply false
    alias(libs.plugins.ksp) apply false
    alias(libs.plugins.protobuf) apply false
    alias(libs.plugins.detekt) apply false
}

allprojects {
    group = "com.latenighthack.lockers"
    version = "0.0.1"
}

subprojects {
    apply(plugin = "io.gitlab.arturbosch.detekt")
    configure<io.gitlab.arturbosch.detekt.extensions.DetektExtension> {
        buildUponDefaultConfig = true
        // Advisory for now: detekt reports issues but does not fail the build.
        // Curate a baseline (./gradlew detektBaseline) then flip this to false to enforce.
        ignoreFailures = true
        basePath = rootProject.projectDir.path
    }
}
