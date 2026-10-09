package com.latenighthack.lockers.server.tools

import com.latenighthack.ktbuf.proto.Enum
import kotlin.test.Test
import kotlin.test.assertEquals

class RpcMetricsTest {
    private object OK : Enum {
        override val value = 0
    }

    private object JOIN_RESULT_OK : Enum {
        override val value = 1
    }

    private object JOIN_RESULT_UNSPECIFIED : Enum {
        override val value = 0
    }

    private object JOIN_RESULT_INVALID_CODE : Enum {
        override val value = 2
    }

    @Test fun successUsesProtocolSymbolRatherThanOrdinal() {
        assertEquals("success", rpcResultOutcome(OK))
        assertEquals("success", rpcResultOutcome(JOIN_RESULT_OK))
        assertEquals("error", rpcResultOutcome(JOIN_RESULT_UNSPECIFIED))
        assertEquals("error", rpcResultOutcome(JOIN_RESULT_INVALID_CODE))
        assertEquals("completed", rpcResultOutcome(null))
    }
}
