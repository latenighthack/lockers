# F38: bounded session stream admission and acknowledgements

Red regressions: `SessionFrameBoundaryTest.oversizedFirstFrameDoesNotReserveASession` submitted a supported create frame plus valid unknown protobuf fields totaling more than 4KiB. The baseline answered OK and reserved authority. `oversizedAckIsRejectedBeforeErasingInboxData` opened a real session with a signed sequence challenge and submitted 257 ACKs; the baseline deleted the pending event.

First frames are now limited to 4KiB before create/open processing; unsupported first-frame variants return INVALID_REQUEST. ACK frames allow at most 256 records and 128KiB, require valid event identities and bounded room identities (empty room remains valid for broadcasts), and fail INVALID_ARGUMENT before durable deletion or delivered-identity pruning. Open sequence signatures require exactly64 bytes and a bounded signature envelope before session lookup and crypto work.

Both actual regressions passed after the fix, alongside session revocation stream tests. Root integration and transport/platform tests remain final gates. This commit preserves ACK-driven pending identity retention and joined producer termination.
