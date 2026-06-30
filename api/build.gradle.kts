plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.protobuf)
    `java-library`
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.33.0"
    }

    generateProtoTasks {
        all().forEach { task ->
            task.builtins {
                removeIf { it.name == "java" }
            }

            // Invokes `protoc-gen-kt` (the ktbuf codegen plugin) found on PATH.
            task.plugins {
                create("kt") {
                    outputSubDir = "kotlin"
                }
            }

            val protoSourceDir: FileCollection = files("${rootDir}/proto")
            task.addSourceDirs(protoSourceDir)
            task.addIncludeDir(protoSourceDir)

            task.outputs.upToDateWhen { false }

            val outputDir = task.outputBaseDir
            if (outputDir.indexOf("/proto/main") > 0) {
                kotlin.sourceSets.getByName("commonMain").kotlin.srcDirs("$outputDir/kotlin")
            }
        }
    }
}

kotlin {
    jvmToolchain(17)

    jvm {
        withJava()
    }

    sourceSets {
        val commonMain by getting {
            dependencies {
                implementation(libs.kotlin.stdlib)
                implementation(libs.ktbuf.library)
                implementation(libs.ktbuf.rpc)
                implementation(libs.coroutines.core)
            }
        }
    }
}

// The generated sources are added as plain srcDirs, so wire the explicit
// task dependency the JVM compile would otherwise be missing.
tasks.matching { it.name == "compileKotlinJvm" || it.name == "jvmSourcesJar" }.configureEach {
    dependsOn("generateProto")
}

// Proto-generated sources are wired only into the per-target (jvm) compile.
// The shared commonMain metadata klib has no consumer here yet, and the
// generated code trips Kotlin metadata compilation, so disable that task.
tasks.matching { it.name == "compileCommonMainKotlinMetadata" }.configureEach {
    enabled = false
}
