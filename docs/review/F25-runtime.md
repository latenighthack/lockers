# F25 — Server cancellation propagation

Three discovered regressions failed: cancellation in push registration became UNKNOWN_ERROR; cancelled session creation became SESSION_EXISTS; cancelled endpoint resolution became a permanent invalid-token rejection. Propagating request-source cancellation also exposed a watch merge whose remaining branches waited forever; stream-level cancellation now ends all branches and unwinds registration.

Fix: cancellation propagates through server unary calls, session create/open, APNs awaiting, and all provider exception boundaries. Blocking FCM/WebPush delivery and DNS resolution use runInterruptible on IO. Temporary DNS failures remain retryable and do not delete browser credentials; security-policy rejections remain permanent. Registry coroutine writes already propagate cancellation from F29. Root owns advisory-lock cancellation handoff; connector owns client cancellation boundaries.

Validation: focused server suite, 33 discovered tests passed. Includes the three cancellation repros (session assertion checks the original cancellation message, excluding a timeout false positive), transient DNS classification, heartbeat, stream lifecycle, credential revisions, and push queue behavior.
