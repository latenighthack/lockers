# F38: bound cumulative protobuf decoding allocation

The input byte/work/depth limits did not prevent generated repeated fields and unknown fields from copying the entire previous collection for every appended element. Two deterministic JVM allocation regressions demonstrate the issue: `GeneratedDecodeAllocationTest.repeatedPackedValuesAccumulateWithoutQuadraticCopies` parsed10,000 packed enum values and allocated400,802,216 bytes; `unknownFieldsAccumulateWithoutQuadraticCopies` parsed30,000 bytes of valid unknown fields and allocated151,772,144 bytes.

The source generator now accumulates repeated fields in typed mutable lists and assigns immutable copies once. Unknown raw field chunks preserve encounter order and flatten once, with checked integer capacity. Existing shared input budgets continue to limit total input/work/depth. There is no runtime API or wire change; generated files remain build outputs. The private installer applies the exact source template replacement after the earlier packed-enum fix, with a one-occurrence guard.

Both allocation tests pass with a16MiB budget each. Whole API signing/model/codegen regressions passed12 JVM,10 Node,10 Apple tests with no failures/skips, plus Android compilation. Paired source generator commits0facc19 and0c3f8ee; Go source compiler check passes. Integrated library/consumer gates remain separate.
