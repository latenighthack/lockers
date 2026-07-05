package com.latenighthack.lockers.keymaster

import com.github.ajalt.clikt.core.CliktCommand
import java.util.Base64

/** Rendering format for command output, selected by the global `--output` flag. */
enum class OutputFormat { TABLE, JSON }

/** Standard base64 (padded) is used in both directions so ids round-trip: an id
 *  printed by `dead-letters list` can be pasted straight into `retry`/`purge`. */
fun ByteArray.toB64(): String = Base64.getEncoder().encodeToString(this)

fun decodeB64(value: String): ByteArray = Base64.getDecoder().decode(value.trim())

/** Emits a flat set of key/value pairs as an aligned table or a JSON object. */
fun CliktCommand.emit(format: OutputFormat, fields: List<Pair<String, Any?>>) {
    when (format) {
        OutputFormat.JSON -> echo(fields.joinToString(prefix = "{", postfix = "}") { (k, v) ->
            "${jsonString(k)}:${jsonValue(v)}"
        })
        OutputFormat.TABLE -> {
            if (fields.isEmpty()) return
            val width = fields.maxOf { it.first.length }
            fields.forEach { (k, v) -> echo("${k.padEnd(width)}  ${v ?: ""}") }
        }
    }
}

/** Emits a list of rows (each a set of key/value pairs) as blocks or a JSON array. */
fun CliktCommand.emitRows(format: OutputFormat, rows: List<List<Pair<String, Any?>>>) {
    when (format) {
        OutputFormat.JSON -> echo(rows.joinToString(prefix = "[", postfix = "]") { row ->
            row.joinToString(prefix = "{", postfix = "}") { (k, v) -> "${jsonString(k)}:${jsonValue(v)}" }
        })
        OutputFormat.TABLE -> {
            if (rows.isEmpty()) {
                echo("(none)")
                return
            }
            rows.forEachIndexed { index, row ->
                if (index > 0) echo("")
                emit(OutputFormat.TABLE, row)
            }
        }
    }
}

private fun jsonValue(value: Any?): String = when (value) {
    null -> "null"
    is Number, is Boolean -> value.toString()
    else -> jsonString(value.toString())
}

private fun jsonString(value: String): String {
    val escaped = value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    return "\"$escaped\""
}
