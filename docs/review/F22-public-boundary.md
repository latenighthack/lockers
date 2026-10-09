# F22 — public signed credential revisions

An authenticated public request with revision zero previously crossed the wrapper
into the trusted compatibility path. A fake trusted handler proves that negative
and zero revisions must never invoke the mutation, while valid signed positive
revisions do. Reject nonpositive revisions before proof consumption. Trusted
private legacy callers retain the separately tested compatibility path.
One discovered regression first fails then passes in
/tmp/lockers-F22-public-red.log and /tmp/lockers-F22-public-green.log.
