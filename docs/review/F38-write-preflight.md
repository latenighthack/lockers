# F38: reject malformed packets before permanent room admission

Two regression groups failed: malformed room/locker identities, negative parents, oversized notification/signature data, ambiguous envelopes and authority packets reached room ownership resolution. This could reserve permanent claim capacity even though the write was subsequently rejected.

Single and batch writes, deletes, lock establishment and unlock now validate supported representations and bounded identity/notification/signature/grant/ratchet structure before ownership lookup or forwarding. Shared signature validation recognizes the existing fixed-width P256 wire signatures and supported signing versions. Valid non-owner routing fixtures now carry structurally valid authority requests.

Evidence: `/tmp/lockers-F38-preflight-red.log` (2 groups failed), `/tmp/lockers-F38-preflight-green.log` (28 targeted preflight, authority, batch, deletion and routing tests pass).
