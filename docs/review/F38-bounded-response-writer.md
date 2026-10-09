# F38: encode response budgets before oversized allocation

With the final paired transport pin, the old complete-response check encoded the entire response using the default 16MiB writer and only then checked the library 8MiB frame limit. A 20MiB bulk response therefore escaped the intended permanent RPC classification as a raw ProtobufOutputLimitException. The isolated b2f0890 baseline fails the strengthened ResponseBoundaryTest expecting RpcResponseException.

The complete-response guard now writes directly through a private 8MiB ProtobufOutputLimits writer and catches typed output/byte-buffer capacity failures as OUT_OF_RANGE. It does not allocate a final validation byte-array copy. Bulk reads and conflicting batch replies reject before writes; immutable paged reads still drain all20 entries.

Validation: strengthened20MiB response/conflict/complete paging test, namespace failure classification and receiver byte/alias tests pass against final ktbuf1.1.10-fh.e4f6e9007a56260dc5d2 and ktstore0.2.0-fh.92be63aec7145a5f4f07. Logs: evidence/F38-response-writer-red.log and evidence/F38-response-writer-green.log.
