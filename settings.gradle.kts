rootProject.name = "lockers"

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

pluginManagement {
    repositories {
        // Global Maven Local is intentionally excluded; use -PfhWorkspace for local development.
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        // Global Maven Local is intentionally excluded; use -PfhWorkspace for local development.
        google()
        mavenCentral()
    }
}

include(":api")
include(":connector")
include(":server")
include(":server:test")
include(":server:run")
include(":keymaster")
include(":sharding-core")

// Explicit isolated library development; release builds use published dependencies.
apply(from = "gradle/fh-workspace.settings.gradle")
