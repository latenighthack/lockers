plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidLibrary)
    alias(libs.plugins.mavenPublish)
}

mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
    // Sign only when a key is configured (CI); local publishToMavenLocal has no signatory.
    if (providers.gradleProperty("signingInMemoryKey").isPresent) {
        signAllPublications()
    }
    coordinates(artifactId = "lockers-api")
    pom {
        name.set("lockers-api")
        description.set("Generated protobuf/gRPC models for the lockers sync API (Kotlin Multiplatform).")
    }
}

// Proto codegen is driven by protoc directly rather than the com.google.protobuf
// Gradle plugin: that plugin only understands java/android source sets and, once
// this became a real multi-target KMP module, bound to per-Android-variant tasks
// instead of producing one shared commonMain output. A single protoc invocation is
// target-agnostic. protoc is resolved as a pinned artifact; `protoc-gen-kt` (the
// ktbuf Kotlin codegen plugin, a Go binary) is installed by the build at a pinned version.
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

// Bootstrap Go may come from PATH; GOTOOLCHAIN pins the compiler used to build
// the plugin. The generator itself is private to this checkout, never global.
val protocGenKtVersion = providers.gradleProperty("protocGenKtVersion").get()
val codegenGoVersion = providers.gradleProperty("codegenGoVersion").get()
val protocGenKt = layout.buildDirectory.file("tools/protoc-gen-kt${if (System.getProperty("os.name").lowercase().contains("win")) ".exe" else ""}")
val installProtocGenKt by tasks.registering(Exec::class) {
    group = "build"
    description = "Install the pinned Kotlin protobuf generator into this build"
    inputs.property("module", "latenighthack.com/protoc-gen-kt@$protocGenKtVersion")
    inputs.property("goToolchain", codegenGoVersion)
    inputs.files(rootProject.files("build-tools/install-protoc-gen-kt.go", "build-tools/protoc-gen-kt-patches.json"))
    outputs.file(protocGenKt)
    environment("GOBIN", protocGenKt.get().asFile.parentFile.absolutePath)
    environment("GOTOOLCHAIN", "go$codegenGoVersion")
    environment("GOPATH", layout.buildDirectory.dir("tooling/gopath").get().asFile.absolutePath)
    environment("GOMODCACHE", layout.buildDirectory.dir("tooling/gomod").get().asFile.absolutePath)
    environment("GOCACHE", layout.buildDirectory.dir("tooling/gocache").get().asFile.absolutePath)
    commandLine("go", "run", rootProject.file("build-tools/install-protoc-gen-kt.go"), protocGenKtVersion,
        rootProject.file("build-tools/protoc-gen-kt-patches.json"), protocGenKt.get().asFile.absolutePath)
}

val generateProto by tasks.registering(Exec::class) {
    group = "build"
    description = "Generate Kotlin protobuf sources via protoc-gen-kt"

    val protoRoot = file("$rootDir/proto")
    val protoFiles = fileTree(protoRoot) { include("**/*.proto") }
    val outDir = layout.buildDirectory.dir("generated/ktproto/kotlin")

    inputs.files(protoFiles)
    dependsOn(installProtocGenKt)
    inputs.file(protocGenKt)
    inputs.files(protocExecutable)
    inputs.property("protocVersion", protocVersion)
    inputs.property("protocClassifier", protocClassifier)
    outputs.dir(outDir)

    doFirst {
        val protoc = protocExecutable.singleFile.apply { setExecutable(true) }
        val out = outDir.get().asFile
        out.deleteRecursively()
        out.mkdirs()

        commandLine(
            buildList {
                add(protoc.absolutePath)
                add("--plugin=protoc-gen-kt=${protocGenKt.get().asFile.absolutePath}")
                add("--kt_out=${out.absolutePath}")
                add("-I")
                add(protoRoot.absolutePath)
                addAll(protoFiles.files.map { it.absolutePath })
            },
        )
    }
}

val generateCodegenFixtures by tasks.registering(Exec::class) {
    val fixture = file("src/commonTest/proto/generator_contract.proto")
    val output = layout.buildDirectory.dir("generated/codegen-test/kotlin")
    val tool = providers.gradleProperty("codegenFixtureGenerator").map { file(it) }.orElse(protocGenKt.map { it.asFile })
    dependsOn(installProtocGenKt)
    inputs.file(fixture)
    inputs.file(tool)
    inputs.files(protocExecutable)
    outputs.dir(output)
    doFirst {
        val protoc = protocExecutable.singleFile.apply { setExecutable(true) }
        output.get().asFile.apply { deleteRecursively(); mkdirs() }
        commandLine(protoc.absolutePath, "--plugin=protoc-gen-kt=${tool.get().absolutePath}",
            "--kt_out=${output.get().asFile.absolutePath}", "-I", fixture.parentFile.absolutePath, fixture.absolutePath)
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
        commonTest { kotlin.srcDir(generateCodegenFixtures) }
        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation(libs.coroutines.test)
        }
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
