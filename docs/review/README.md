# Correctness and coroutine review remediation

This branch preserves the existing working source before any remediation. The
original checkout is not modified. Each finding is resolved by a distinct commit
with a reproduction, regression coverage, fix and recorded validation. A status
of `open` or `partial` is not completion. Final verification includes the library
suite and Fullhouse consumers on JVM, Android, JavaScript and Apple targets.

See `findings.json` for the complete burndown and `review.md` for the original
findings. Evidence for individual findings belongs in `Fxx.md`. Record failing
reproduction commands and passing regression commands, including limitations.

The work proceeds in dependency order: trust boundary validation; authority and
atomic persistence; session and delivery durability; coroutine ownership and
client observation; resource bounds; reproducible builds and consumer tests.

Do not edit generated bindings or frozen storage schemas. Signing-domain and
storage changes require explicit versions. Preserve public extension boundaries
and use isolated dependency repositories rather than global Maven Local.
