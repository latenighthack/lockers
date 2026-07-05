package com.latenighthack.lockers.keymaster

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.ProgramResult
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.parameters.options.option
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

private fun KeymasterConfig.getText(path: String): HttpResponse<String> {
    val uri = URI.create(publicUrl.trimEnd('/') + path)
    val request = HttpRequest.newBuilder(uri).GET().build()
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
}

/** `health` — probes /healthz and /readyz on the public port. */
class HealthCommand : CliktCommand(name = "health") {
    private val config by requireObject<KeymasterConfig>()
    override fun help(context: Context) = "Probe the server's /healthz and /readyz endpoints."
    override fun run() {
        val fields = try {
            listOf("/healthz", "/readyz").map { path ->
                val response = config.getText(path)
                path to "${response.statusCode()} ${response.body().trim()}"
            }
        } catch (e: IOException) {
            echo("health check failed: ${e.message} (${config.publicUrl})", err = true)
            throw ProgramResult(1)
        } catch (e: InterruptedException) {
            echo("health check interrupted: ${e.message}", err = true)
            throw ProgramResult(1)
        }
        emit(config.output, fields)
    }
}

/** `metrics` — scrapes the Prometheus /metrics endpoint, optionally line-filtered. */
class MetricsCommand : CliktCommand(name = "metrics") {
    private val config by requireObject<KeymasterConfig>()
    override fun help(context: Context) = "Scrape /metrics (Prometheus text), optionally filtered by substring."

    private val filter by option("--filter", help = "Only print lines containing this substring.")

    override fun run() {
        val body = try {
            config.getText("/metrics").body()
        } catch (e: IOException) {
            echo("metrics scrape failed: ${e.message} (${config.publicUrl})", err = true)
            throw ProgramResult(1)
        } catch (e: InterruptedException) {
            echo("metrics scrape interrupted: ${e.message}", err = true)
            throw ProgramResult(1)
        }
        val needle = filter
        body.lineSequence()
            .filter { needle == null || it.contains(needle) }
            .forEach { echo(it) }
    }
}
