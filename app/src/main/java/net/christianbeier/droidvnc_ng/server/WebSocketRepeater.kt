/*
 * droidVNC-NG WebSocket repeater connection.
 *
 * You can redistribute and/or modify this program under the terms of the
 * GNU General Public License version 2 as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
 * Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place Suite 330, Boston, MA 02111-1307, USA.
 */

package net.christianbeier.droidvnc_ng.server

import android.os.ParcelFileDescriptor
import android.system.ErrnoException
import android.system.Os
import android.system.OsConstants
import android.util.Log
import androidx.annotation.WorkerThread
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import okio.ByteString.Companion.toByteString
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Outbound connection to an UltraVNC-style Mode-2 repeater that is reached via WebSocket
 * (`ws://` or `wss://`), e.g. the `/relay/server` endpoint of vnc-relay.
 *
 * LibVNCServer only speaks plain sockets, so the WebSocket is bridged to one end of a local
 * socket pair while the other end is handed to LibVNCServer as an already connected repeater
 * client. This keeps the LibVNCServer submodule untouched; see ADR 0006.
 *
 * On the wire: binary messages only, `Sec-WebSocket-Protocol: binary` (plus `ticket.<JWS>` if an
 * access ticket is given), then the 250-byte `ID:<id>` request first, then the RFB stream.
 */
object WebSocketRepeater {

    private const val TAG = "WebSocketRepeater"

    /** Path used when the URL has none, vnc-relay's server-role endpoint. */
    const val DEFAULT_PATH = "/relay/server"

    /** Size of the UltraVNC repeater ID request, NUL-padded. */
    private const val ID_MESSAGE_SIZE = 250
    private const val ID_PREFIX = "ID:"
    private const val SUBPROTOCOL_BINARY = "binary"
    private const val SUBPROTOCOL_TICKET_PREFIX = "ticket."
    private val JWS_COMPACT = Regex("^[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$")

    /** Upper bound for TCP connect, TLS and HTTP upgrade together. */
    private const val HANDSHAKE_TIMEOUT_S = 15L
    private const val PING_INTERVAL_S = 30L

    /** Bytes read from LibVNCServer per WebSocket message. vnc-relay accepts messages of at least 64 KiB. */
    private const val READ_CHUNK_SIZE = 64 * 1024

    /** Stop reading from LibVNCServer while this much is queued for sending, so it blocks on its socket instead. */
    private const val SEND_QUEUE_HIGH_WATER = 1L shl 20
    private const val SEND_QUEUE_POLL_MS = 10L

    private const val CLOSE_NORMAL = 1000
    private const val CLOSE_UNSUPPORTED_DATA = 1003

    private val httpClient: OkHttpClient by lazy {
        OkHttpClient.Builder()
            // only bounds the handshake, OkHttp lifts it once the WebSocket is open
            .callTimeout(HANDSHAKE_TIMEOUT_S, TimeUnit.SECONDS)
            .pingInterval(PING_INTERVAL_S, TimeUnit.SECONDS)
            .build()
    }

    /**
     * Hands the LibVNCServer end of the socket pair over to native code.
     * Ownership of [fd] passes to the callee, also on failure.
     * @return the new client pointer or 0 on failure
     */
    fun interface ClientAdopter {
        fun adopt(fd: Int, host: String, port: Int, repeaterId: String): Long
    }

    /**
     * True if [host] is a `ws://` or `wss://` URL rather than a plain host name.
     */
    @JvmStatic
    fun isWebSocketUrl(host: String?): Boolean =
        host != null && (host.startsWith("ws://", ignoreCase = true) || host.startsWith("wss://", ignoreCase = true))

    /**
     * The effective port of a `ws://` or `wss://` URL, or -1 if it does not parse.
     */
    @JvmStatic
    fun getPort(url: String): Int = toHttpUrl(url)?.port ?: -1

    /**
     * Connects to the WebSocket repeater at [url], registers with [repeaterId] and hands the bridged
     * connection to [adopter]. Blocks until the connection is set up or failed.
     *
     * @param accessTicket optional JWS compact access ticket, sent as `ticket.<JWS>` subprotocol.
     *                     Tickets are single-use, so callers must not pass the same one twice.
     * @return the client pointer from [adopter] or 0 on failure
     */
    @JvmStatic
    @WorkerThread
    fun connect(url: String, repeaterId: String, accessTicket: String?, adopter: ClientAdopter): Long {
        val httpUrl = toHttpUrl(url)
        if (httpUrl == null) {
            Log.e(TAG, "connect: not a valid ws:// or wss:// URL")
            return 0
        }
        val idMessage = idMessage(repeaterId)
        if (idMessage == null) {
            Log.e(TAG, "connect: repeater ID empty, not ASCII or too long")
            return 0
        }
        if (accessTicket != null && !JWS_COMPACT.matches(accessTicket)) {
            // never log the ticket itself
            Log.e(TAG, "connect: access ticket is not in JWS compact format")
            return 0
        }

        val subprotocols = if (accessTicket != null) "$SUBPROTOCOL_BINARY, $SUBPROTOCOL_TICKET_PREFIX$accessTicket" else SUBPROTOCOL_BINARY
        val request = Request.Builder()
            .url(httpUrl)
            .header("Sec-WebSocket-Protocol", subprotocols)
            .build()

        val socketPair = try {
            ParcelFileDescriptor.createSocketPair()
        } catch (e: IOException) {
            Log.e(TAG, "connect: could not create socket pair: $e")
            return 0
        }
        val bridge = Bridge(socketPair[0])
        val webSocket = httpClient.newWebSocket(request, bridge)

        if (!bridge.awaitOpen()) {
            webSocket.cancel()
            bridge.shutdown()
            bridge.closeLocal()
            socketPair[1].close()
            return 0
        }

        // the repeater expects the ID request before anything else
        webSocket.send(idMessage)

        val client = adopter.adopt(socketPair[1].detachFd(), url, httpUrl.port, repeaterId)
        if (client == 0L) {
            Log.e(TAG, "connect: LibVNCServer did not adopt the connection")
            webSocket.close(CLOSE_NORMAL, null)
            bridge.shutdown()
            bridge.closeLocal()
            return 0
        }

        bridge.startPump(webSocket)
        Log.i(TAG, "connect: connected to ${httpUrl.host}:${httpUrl.port}${httpUrl.encodedPath}")
        return client
    }

    private fun toHttpUrl(url: String): HttpUrl? {
        // OkHttp's HttpUrl only knows http(s), Request.Builder maps ws(s) the same way
        val httpUrl = when {
            url.startsWith("ws://", ignoreCase = true) -> "http://" + url.substring(5)
            url.startsWith("wss://", ignoreCase = true) -> "https://" + url.substring(6)
            else -> return null
        }.toHttpUrlOrNull() ?: return null
        return if (httpUrl.encodedPath == "/") httpUrl.newBuilder().encodedPath(DEFAULT_PATH).build() else httpUrl
    }

    private fun idMessage(repeaterId: String): ByteString? {
        // toByteArray(US_ASCII) would silently replace non-ASCII characters
        if (repeaterId.isEmpty() || repeaterId.any { it.code > 0x7f }) return null
        val id = (ID_PREFIX + repeaterId).toByteArray(Charsets.US_ASCII)
        if (id.size > ID_MESSAGE_SIZE) return null
        return id.copyOf(ID_MESSAGE_SIZE).toByteString()
    }

    /**
     * Pumps bytes between the WebSocket and the local end of the socket pair.
     * Either side going away tears down the other: LibVNCServer closing its end ends the
     * WebSocket, the WebSocket ending makes LibVNCServer read EOF.
     */
    private class Bridge(private val local: ParcelFileDescriptor) : WebSocketListener() {

        private val opened = CountDownLatch(1)
        @Volatile private var openSucceeded = false
        private val isShutdown = AtomicBoolean(false)
        private val toLocal = FileOutputStream(local.fileDescriptor)

        fun awaitOpen(): Boolean {
            try {
                // callTimeout bounds the handshake, the margin covers callback delivery
                if (!opened.await(HANDSHAKE_TIMEOUT_S + 5, TimeUnit.SECONDS)) {
                    Log.e(TAG, "connect: handshake timed out")
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
            }
            return openSucceeded
        }

        /** Reads what LibVNCServer writes and sends it as binary messages. Owns closing the local end. */
        fun startPump(webSocket: WebSocket) {
            Thread({
                val fromLocal = FileInputStream(local.fileDescriptor)
                val buffer = ByteArray(READ_CHUNK_SIZE)
                try {
                    while (true) {
                        val n = fromLocal.read(buffer)
                        if (n < 0) break
                        while (webSocket.queueSize() > SEND_QUEUE_HIGH_WATER && !isShutdown.get()) {
                            Thread.sleep(SEND_QUEUE_POLL_MS)
                        }
                        if (!webSocket.send(buffer.toByteString(0, n))) break
                    }
                } catch (e: IOException) {
                    Log.d(TAG, "pump: local read ended: $e")
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
                // LibVNCServer closed the client or the WebSocket went away; no-op in the latter case
                webSocket.close(CLOSE_NORMAL, null)
                shutdown()
                closeLocal()
            }, TAG).start()
        }

        /** Makes LibVNCServer read EOF and unblocks the pump. */
        fun shutdown() {
            if (!isShutdown.compareAndSet(false, true)) return
            try {
                Os.shutdown(local.fileDescriptor, OsConstants.SHUT_RDWR)
            } catch (ignored: ErrnoException) {
                // already disconnected
            }
        }

        fun closeLocal() {
            try {
                local.close()
            } catch (ignored: IOException) {
            }
        }

        override fun onOpen(webSocket: WebSocket, response: Response) {
            openSucceeded = true
            opened.countDown()
        }

        override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
            try {
                // blocking write: backpressure towards the repeater while LibVNCServer is busy
                bytes.write(toLocal)
            } catch (e: IOException) {
                Log.d(TAG, "onMessage: local write failed: $e")
                webSocket.cancel()
                shutdown()
            }
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
            Log.e(TAG, "onMessage: unexpected text message, closing")
            webSocket.close(CLOSE_UNSUPPORTED_DATA, null)
            shutdown()
        }

        override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
            // reason is a fixed identifier like 'wait_timeout' or 'ticket_relay_mismatch', no secrets
            Log.i(TAG, "onClosing: repeater closed with $code $reason")
            // echo the code as RFC 6455 suggests; codes like 1005 (none received) must not be sent
            try {
                webSocket.close(code, null)
            } catch (e: IllegalArgumentException) {
                webSocket.close(CLOSE_NORMAL, null)
            }
            shutdown()
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            shutdown()
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            if (response != null) {
                // e.g. 401 for a missing, invalid or reused ticket, 403/404 for access or path errors
                Log.e(TAG, "onFailure: HTTP ${response.code}: $t")
            } else {
                Log.e(TAG, "onFailure: $t")
            }
            shutdown()
            opened.countDown()
        }
    }
}
