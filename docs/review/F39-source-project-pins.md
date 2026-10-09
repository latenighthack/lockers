# Workspace pins preserve source projects

The paired ktbuf publication reproduced eachDependency overrides converting source project edges into absent external project-name artifacts. The shared workspace script now recognizes group/name pairs belonging to this build, including cross-group observability projects, and preserves those source edges. External dependencies still resolve exclusively from immutable manifest coordinates.

:connector:jsPackageJson and :observability-connector:jsPackageJson pass with workspace overrides whose lockers/observability artifacts deliberately do not exist. This verifies a source build can bootstrap publication without consulting global Maven Local or a previous binary version. The same correction allowed all178 ktbuf artifacts to publish through the isolated Fullhouse CLI.
