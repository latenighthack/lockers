rootProject.name = "lockers"

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

pluginManagement {
    repositories {
        mavenLocal()
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenLocal()
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
