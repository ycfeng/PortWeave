package dev.portweave.core.data.transfer

import java.io.ByteArrayOutputStream

/** RFC 9285 Base45, whose alphabet stays inside QR alphanumeric mode. */
internal object Base45 {
    private const val Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ $%*+-./:"
    private val reverse = IntArray(128) { -1 }.also { values ->
        Alphabet.forEachIndexed { index, character -> values[character.code] = index }
    }

    fun encode(input: ByteArray): String = buildString((input.size * 3 + 1) / 2) {
        var index = 0
        while (index + 1 < input.size) {
            val value = (input[index].toInt() and 0xff) * 256 + (input[index + 1].toInt() and 0xff)
            append(Alphabet[value % 45])
            append(Alphabet[value / 45 % 45])
            append(Alphabet[value / (45 * 45)])
            index += 2
        }
        if (index < input.size) {
            val value = input[index].toInt() and 0xff
            append(Alphabet[value % 45])
            append(Alphabet[value / 45])
        }
    }

    fun decode(input: String): ByteArray {
        require(input.length % 3 != 1) { "二维码 Base45 长度无效" }
        val output = ByteArrayOutputStream(input.length * 2 / 3)
        var index = 0
        while (index + 2 < input.length) {
            val value = digit(input[index]) + digit(input[index + 1]) * 45 + digit(input[index + 2]) * 45 * 45
            require(value <= 0xffff) { "二维码 Base45 数据无效" }
            output.write(value / 256)
            output.write(value % 256)
            index += 3
        }
        if (index < input.length) {
            val value = digit(input[index]) + digit(input[index + 1]) * 45
            require(value <= 0xff) { "二维码 Base45 数据无效" }
            output.write(value)
        }
        return output.toByteArray()
    }

    private fun digit(character: Char): Int {
        val value = if (character.code < reverse.size) reverse[character.code] else -1
        require(value >= 0) { "二维码 Base45 字符无效" }
        return value
    }
}
