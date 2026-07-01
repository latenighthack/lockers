plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidLibrary)
    `maven-publish`
}

publishing {
    repositories {
        mavenLocal()
    }
}

// Proto codegen is driven by protoc directly rather than the com.google.protobuf
// Gradle plugin: that plugin only understands java/android source sets and, once
// this became a real multi-target KMP module, bound to per-Android-variant tasks
// instead of producing one shared commonMain output. A single protoc invocation is
// target-agnostic. protoc is resolved as a pinned artifact; `protoc-gen-kt` (the
// ktbuf Kotlin codegen plugin, a Go binary) is discovered on PATH (~/go/bin fallback).
val protocVersion = "4.33.0"

val protocClassifier: String = run {
    val os = System.getProperty("os.name").lowercase()
    val arch = System.getProperty("os.arch").lowercase()
    val osPart = when {
        os.contains("mac") || os.contains("darwin") -> "osx"
        os.contains("win") -> "windows"
        else -> "linux"
    }
    val archPart = when (arch) {
        "aarch64", "arm64" -> "aarch_64"
        "x86_64", "amd64" -> "x86_64"
        else -> arch
    }
    "$osPart-$archPart"
}

val protocExecutable: Configuration by configurations.creating
dependencies {
    protocExecutable("com.google.protobuf:protoc:$protocVersion:$protocClassifier@exe")
}

val protocGenKt: File = run {
    val onPath = System.getenv("PATH").orEmpty()
        .split(File.pathSeparator)
        .map { File(it, "protoc-gen-kt") }
        .firstOrNull { it.canExecute() }
    onPath ?: File(System.getProperty("user.home"), "go/bin/protoc-gen-kt")
}

val generateProto by tasks.registering(Exec::class) {
    group = "build"
    description = "Generate Kotlin protobuf sources via protoc-gen-kt"

    val protoRoot = file("$rootDir/proto")
    val protoFiles = fileTree(protoRoot) { include("**/*.proto") }
    val outDir = layout.buildDirectory.dir("generated/ktproto/kotlin")

    inputs.files(protoFiles)
    inputs.file(protocGenKt)
    outputs.dir(outDir)

    doFirst {
        val protoc = protocExecutable.singleFile.apply { setExecutable(true) }
        val out = outDir.get().asFile
        out.deleteRecursively()
        out.mkdirs()

        commandLine(
            buildList {
                add(protoc.absolutePath)
                add("--plugin=protoc-gen-kt=${protocGenKt.absolutePath}")
                add("--kt_out=${out.absolutePath}")
                add("-I")
                add(protoRoot.absolutePath)
                addAll(protoFiles.files.map { it.absolutePath })
            },
        )
    }
}

kotlin {
    jvmToolchain(17)

    jvm()
    androidTarget { publishLibraryVariants("release") }
    iosArm64()
    iosX64()
    iosSimulatorArm64()
    js(IR) {
        browser()
        nodejs()
        binaries.library()
    }

    sourceSets {
        val commonMain by getting {
            // Passing the task provider wires the generateProto dependency into every
            // compilation that reads commonMain (metadata klib + each per-target compile)
            // and the sources jars, with no manual dependsOn needed.
            kotlin.srcDir(generateProto)
            dependencies {
                implementation(libs.kotlin.stdlib)
                implementation(libs.ktbuf.library)
                implementation(libs.ktbuf.rpc)
                implementation(libs.coroutines.core)
            }
        }
    }
}

android {
    namespace = "com.latenighthack.lockers.api"
    compileSdk = 35
    defaultConfig {
        minSdk = 24
    }
}
