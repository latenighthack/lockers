# F02 stored and extension-produced envelope validation

Two red regressions reproduced malformed/ambiguous stored bodies escaping reads and ambiguous trusted agent output being committed. Reads now validate supported representations before returning a body. INVALID_DATA (additive enum value 2) carries locker identity, keyspace and version for owner repair while withholding the malformed body. Bulk/snapshot responses fail as a whole and report invalid metadata; they do not silently present an incomplete snapshot. No stored bytes are deleted or rewritten.

The malformed-byte regression also found the generated decoder zero-padding an incomplete length-delimited submessage. A bounded structural scan validates known nested Locker messages and their lengths before decode; legitimate unknown scalar/byte fields are retained. Unsupported historical group wire fields require owner repair. Derived output validation runs before any derived row or event commit; source writes retain their durable success and report agent_failed.

Both regressions pass across single, bulk, whole-room and subscription snapshot reads plus derived-agent acceptance. API JVM/Node/Apple tests ran with the additive response enums. Red/green server logs are in evidence/F02-read. Upstream ktbuf request decoding needs its own allocation/length hardening before application trust-boundary limits can apply.
