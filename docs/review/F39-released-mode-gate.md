# F39: released-mode resolution remains an external release gate

A clean separate worktree at `40b705d` ran the repository wrapper without `-PfhWorkspace` or a local Maven repository override:

`ANDROID_HOME=/Users/mikeroberts/Library/Android/sdk ./gradlew :connector:compileKotlinJvm :server:compileKotlin --console=plain`

The check failed resolving `com.latenighthack.ktstore:ktstore-library:0.2.0` from the declared public repositories. It did not consult global Maven Local. The library source also requires the reviewed paired ktbuf/ktstore APIs and fixes recorded in F39-release-transition.md. The successful isolated local dependency build is not a default released-mode pass.

The concrete next release stages are recorded in F39-release-transition.md; neither external publication nor service deployment is part of local validation. The final library and consumer private gates continue before this remaining external prerequisite is reported. Raw output: evidence/F39-released-mode-gate.log.
