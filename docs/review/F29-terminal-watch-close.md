# F29: a Close frame terminates its Flow

The revocation regression now collects the complete Flow rather than stopping at the first Close frame. It timed out before the fix: emitting Close did not stop request intake, replay and polling branches.

After Close reaches the output channel, the owned producer cancels and joins its merged branches, releases stream registration, and completes normally. Close and preceding response frames remain available to the collector. The full Flow revocation test, joined-cleanup test, inbox paging/receipts and component shutdown regressions pass in `/tmp/lockers-F29-terminal-close-green.log`; the baseline failure is `/tmp/lockers-F29-terminal-close-red.log`.
