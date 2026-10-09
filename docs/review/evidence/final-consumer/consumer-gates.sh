#!/bin/zsh
set -euo pipefail
# Run only after the immutable Locker publication and root final checkpoint.
consumer_review_root=/Users/mikeroberts/workspace/rollgames/.worktrees/fullhouse-lockers-review-build
consumer_review_manifest=${1:?Pass the frozen final FH dependency manifest path}
consumer_review_generator=/Users/mikeroberts/workspace/latenighthack/.worktrees/ktbuf-review-publish/conformance/build/tools
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export ANDROID_HOME=/Users/mikeroberts/Library/Android/sdk
export PATH="$consumer_review_generator:$PATH"
cd "$consumer_review_root/project"
consumer_review_flags=("-PfhWorkspace=$consumer_review_manifest" "-Dmaven.repo.local=$consumer_review_root/.fh/maven")
./gradlew :server:jvmTest :core:test:jvmTest :server:run:compileKotlin "${consumer_review_flags[@]}" > /tmp/fullhouse-lockers-final-jvm.log 2>&1
./gradlew :core:compileDebugKotlinAndroid "${consumer_review_flags[@]}" > /tmp/fullhouse-lockers-final-android.log 2>&1
./gradlew :core:jsBrowserProductionLibraryDistribution "${consumer_review_flags[@]}" > /tmp/fullhouse-lockers-final-js.log 2>&1
./gradlew :core:linkDebugFrameworkIosSimulatorArm64 "${consumer_review_flags[@]}" > /tmp/fullhouse-lockers-final-apple.log 2>&1
