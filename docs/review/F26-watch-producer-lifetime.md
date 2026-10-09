# F26: collected stream failure and joined cleanup

Two regressions failed: an attachment failure reached its Flow collector but also escaped to the host coroutine exception handler; ending a watch collection returned before non-cancellable upstream cleanup finished.

The service-owned watch producer now captures failures in a deferred child and propagates them through the output channel. Cancelling collection cancels and joins that producer in non-cancellable cleanup. Host shutdown still cancels the service lifetime; collectors retain their own cancellation and failure semantics.

Evidence: `/tmp/lockers-F26-watch-failure-red.log` (3 tests, 2 failed) and `/tmp/lockers-F26-watch-failure-green.log` (attachment, ownership, shutdown and cancellation tests pass).
