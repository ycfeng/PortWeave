plugins {
    alias(libs.plugins.android.application) apply false
    alias(libs.plugins.android.library) apply false
    alias(libs.plugins.kotlin.compose) apply false
    alias(libs.plugins.ksp) apply false
    alias(libs.plugins.kotlin.serialization) apply false
}

val checkedSourceExtensions = setOf("kt", "kts", "xml", "yml", "yaml", "md")

tasks.register("checkFormatting") {
    group = "verification"
    description = "Checks text files for tabs, trailing whitespace, and a final newline."
    doLast {
        val failures = mutableListOf<String>()
        fileTree(rootDir) {
            exclude(".gradle/**", ".git/**", "**/build/**", "doc/**")
        }.files.filter { it.isFile && it.extension in checkedSourceExtensions }.forEach { file ->
            val text = file.readText()
            if ('\t' in text) failures += "${file.relativeTo(rootDir)}: contains a tab"
            text.lineSequence().forEachIndexed { index, line ->
                if (line != line.trimEnd()) failures += "${file.relativeTo(rootDir)}:${index + 1}: trailing whitespace"
            }
            if (text.isNotEmpty() && !text.endsWith("\n")) failures += "${file.relativeTo(rootDir)}: missing final newline"
        }
        check(failures.isEmpty()) { failures.joinToString(separator = "\n") }
    }
}

tasks.register("verifyLicenses") {
    group = "verification"
    description = "Checks that direct third-party dependencies have license notices."
    doLast {
        val notice = file("THIRD_PARTY_NOTICES.md")
        check(notice.isFile) { "THIRD_PARTY_NOTICES.md is missing" }
        val content = notice.readText()
        listOf(
            "AndroidX",
            "Kotlin",
            "Material",
            "Room",
            "DataStore",
            "JSch",
            "Bouncy Castle",
            "Apache MINA SSHD",
            "OC Deck pure-Kotlin frpc STCP visitor",
            "frp protocol",
        ).forEach { component ->
            check(component in content) { "Missing license notice for $component" }
        }
        listOf(
            "LICENSE",
            "NOTICE",
            "PRIVACY.md",
            "release-notes/v0.1.0.md",
            "third_party/licenses/Apache-2.0.txt",
            "third_party/licenses/BSD-3-Clause.txt",
            "third_party/licenses/EPL-1.0.txt",
            "third_party/licenses/MIT.txt",
            "app/build.gradle.kts",
            "signing.properties.example",
        ).forEach { path ->
            check(file(path).isFile) { "Required release/legal file is missing: $path" }
        }
        check("generateLegalAssets" in file("app/build.gradle.kts").readText()) {
            "Release APK must generate and bundle offline legal notices"
        }
    }
}

tasks.register("releaseQualityCheck") {
    group = "verification"
    description = "Runs the host-side Stage F release-quality gates."
    dependsOn(
        "checkFormatting",
        "verifyLicenses",
        ":core:testDebugUnitTest",
        ":tunnel-engine:testDebugUnitTest",
        ":frpc-stcp-visitor:testDebugUnitTest",
        ":app:testDebugUnitTest",
        ":app:lintRelease",
        ":app:verifyReleasePackaging",
        ":app:assembleDebugAndroidTest",
    )
}
