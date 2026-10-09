package gg.roll.viewmodel.core

import com.latenighthack.ktbuf.test.server.runTestWithServer
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.fromPrivateKey
import com.latenighthack.ktcrypto.encode
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.room.v1.GetLockerRequest
import com.latenighthack.lockers.room.v1.RoomServiceRpc
import com.latenighthack.social.account.domain.AccountManager
import com.latenighthack.social.profiles.v1.ProfileSource
import com.latenighthack.social.profiles.v1.fromByteArray
import gg.roll.fullhouse.server.attachTestServices
import gg.roll.viewmodel.core.tools.runCoreTest
import io.ktor.server.application.Application
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Separate red security probe for pre-existing application policy debt, not the Locker library gate. */
class ConsumerReadPolicyDebtProbe {
    @Test(timeout = 30_000)
    fun `a client without session credentials must not recover an account profile signing key`() =
        runTestWithServer(Application::attachTestServices) { server, _ ->
            runCoreTest(server.rpcClient()) {
                preAuthenticated()
                run { component, _, _ ->
                    val core = component as BaseCore
                    val account = core.social.accountManager.lifecycle.filterIsInstance<AccountManager.Lifecycle.Ready>().first()
                    core.social.myProfilesManager.isLoaded.first { it }
                    val profile = core.social.myProfilesManager.createProfile("Boundary probe")
                    // This fresh direct RPC client has no session proof or application account credentials.
                    val anonymous = RoomServiceRpc(server.rpcClient())
                    val response = anonymous.getLocker(GetLockerRequest(
                        roomId = account.privateRoom,
                        lockerId = LockerId(profile.rawValue, LockerKeyspace(2)),
                    ))
                    assertTrue(response.result.isOk(), "Default public read policy is reachable")
                    val plaintext = response.locker?.locker?.sealed?.payload?.enclosure?.innerPayload
                    val source = plaintext?.let(ProfileSource::fromByteArray)
                    val reconstructed = source?.let { Secp256r1KeyPair.fromPrivateKey(it.privateKey) }
                    val recovered = reconstructed?.publicKey?.encode()?.contentEquals(profile.rawValue) == true
                    // Boolean assertion deliberately avoids printing any private key or payload bytes.
                    assertFalse(recovered, "Unauthenticated public read recovered the account's profile signing key")
                }
            }
        }
}
