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
        // Workspace mode prefers settings repositories; retain Kotlin/JS tool downloads.
        ivy {
            name = "nodeDistributions"
            url = uri("https://nodejs.org/dist")
            patternLayout { artifact("v[revision]/[artifact](-v[revision]-[classifier]).[ext]") }
            metadataSources { artifact() }
            content { includeModule("org.nodejs", "node") }
        }
        ivy {
            name = "yarnDistributions"
            url = uri("https://github.com/yarnpkg/yarn/releases/download")
            patternLayout { artifact("v[revision]/[artifact](-v[revision]).[ext]") }
            metadataSources { artifact() }
            content { includeModule("com.yarnpkg", "yarn") }
        }
    }
}

include(":api")
include(":connector")
include(":server")
include(":server:test")
include(":server:run")
include(":keymaster")
include(":sharding-core")
include(":observability-api", ":observability-connector", ":observability-server")

// Explicit isolated library development; release builds use published dependencies.
apply(from = "gradle/fh-workspace.settings.gradle")
