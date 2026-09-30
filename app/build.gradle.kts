import com.android.build.api.variant.FilterConfiguration.FilterType.ABI
import java.util.Properties
import java.util.zip.ZipFile

plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.compose)
}

val apkApplicationName = "PortWeave"
val stableVersionPattern = Regex("""(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)""")
val appVersionName = providers.gradleProperty("VERSION_NAME").orNull
    ?.trim()
    ?.takeIf(stableVersionPattern::matches)
    ?: throw GradleException("VERSION_NAME must be a stable semantic version such as 1.0.0")
val appVersionCode = providers.gradleProperty("VERSION_CODE").orNull
    ?.trim()
    ?.toIntOrNull()
    ?.takeIf { it in 1..2_100_000_000 }
    ?: throw GradleException("VERSION_CODE must be an integer between 1 and 2100000000")
val releaseSigningPropertiesFile = rootProject.file("signing.properties")
val releaseSigningProperties = Properties().apply {
    if (releaseSigningPropertiesFile.isFile) {
        releaseSigningPropertiesFile.inputStream().use(::load)
    }
}

fun releaseSigningValue(name: String): String? =
    providers.environmentVariable(name).orNull?.takeIf { it.isNotBlank() }
        ?: releaseSigningProperties.getProperty(name)?.takeIf { it.isNotBlank() }

val releaseSigningValues = mapOf(
    "RELEASE_STORE_FILE" to releaseSigningValue("RELEASE_STORE_FILE"),
    "RELEASE_STORE_PASSWORD" to releaseSigningValue("RELEASE_STORE_PASSWORD"),
    "RELEASE_KEY_ALIAS" to releaseSigningValue("RELEASE_KEY_ALIAS"),
    "RELEASE_KEY_PASSWORD" to releaseSigningValue("RELEASE_KEY_PASSWORD"),
)
val releaseSigningConfigured = releaseSigningValues.values.all { !it.isNullOrBlank() }
val generatedLegalAssets = layout.buildDirectory.dir("generated/legalAssets")
val generateLegalAssets = tasks.register("generateLegalAssets") {
    val licenseDirectory = rootProject.layout.projectDirectory.dir("third_party/licenses")
    val outputFile = generatedLegalAssets.map { it.file("licenses.txt") }
    inputs.files(
        rootProject.file("LICENSE"),
        rootProject.file("NOTICE"),
        rootProject.file("THIRD_PARTY_NOTICES.md"),
    )
    inputs.dir(licenseDirectory)
    outputs.file(outputFile)
    doLast {
        val licenseFiles = licenseDirectory.asFile.listFiles()
            ?.filter { it.isFile }
            ?.sortedBy { it.name }
            .orEmpty()
        check(licenseFiles.isNotEmpty()) { "No third-party license files found" }
        val sections = listOf(
            "PORTWEAVE LICENSE" to rootProject.file("LICENSE"),
            "PORTWEAVE NOTICE" to rootProject.file("NOTICE"),
            "THIRD-PARTY NOTICES" to rootProject.file("THIRD_PARTY_NOTICES.md"),
        ) + licenseFiles.map { "THIRD-PARTY LICENSE: ${it.name}" to it }
        val content = buildString {
            appendLine("PortWeave offline legal notices")
            sections.forEach { (title, file) ->
                appendLine()
                appendLine("================================================================================")
                appendLine(title)
                appendLine("================================================================================")
                append(file.readText(Charsets.UTF_8).trimEnd())
                appendLine()
            }
        }
        outputFile.get().asFile.apply {
            parentFile.mkdirs()
            writeText(content, Charsets.UTF_8)
        }
    }
}

android {
    namespace = "dev.portweave.app"
    compileSdk = 36

    defaultConfig {
        applicationId = "dev.portweave.app"
        minSdk = 26
        targetSdk = 36
        versionCode = appVersionCode
        versionName = appVersionName
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    splits {
        abi {
            isEnable = true
            reset()
            include("arm64-v8a", "armeabi-v7a", "x86_64")
            isUniversalApk = false
        }
    }

    signingConfigs {
        if (releaseSigningConfigured) {
            create("release") {
                storeFile = rootProject.file(releaseSigningValues.getValue("RELEASE_STORE_FILE")!!)
                storePassword = releaseSigningValues.getValue("RELEASE_STORE_PASSWORD")!!
                keyAlias = releaseSigningValues.getValue("RELEASE_KEY_ALIAS")!!
                keyPassword = releaseSigningValues.getValue("RELEASE_KEY_PASSWORD")!!
            }
        }
    }

    buildTypes {
        debug {
            applicationIdSuffix = ".debug"
            versionNameSuffix = "-debug"
        }
        release {
            if (releaseSigningConfigured) {
                signingConfig = signingConfigs.getByName("release")
            }
            isMinifyEnabled = true
            isShrinkResources = true
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro",
            )
        }
        create("reproducibleRelease") {
            initWith(getByName("release"))
            matchingFallbacks += "release"
            applicationIdSuffix = ".repro"
            versionNameSuffix = "-repro"
            signingConfig = null
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }

    buildFeatures.compose = true
    sourceSets.getByName("main").assets.directories.add(generatedLegalAssets.get().asFile.absolutePath)
    packaging.resources.excludes += "/META-INF/{AL2.0,LGPL2.1}"
}

tasks.named("preBuild").configure { dependsOn(generateLegalAssets) }

val validateReleaseSigning = tasks.register("validateReleaseSigning") {
    group = "verification"
    description = "Checks that release signing credentials and the keystore are configured."
    doLast {
        val missingKeys = releaseSigningValues.filterValues { it.isNullOrBlank() }.keys
        if (missingKeys.isNotEmpty()) {
            throw GradleException(
                "Release signing is not configured. Missing: ${missingKeys.joinToString()}. " +
                    "Set environment variables or create ignored root signing.properties from signing.properties.example.",
            )
        }

        val storeFile = rootProject.file(releaseSigningValues.getValue("RELEASE_STORE_FILE")!!)
        if (!storeFile.isFile) {
            throw GradleException("Release signing store file does not exist: ${storeFile.absolutePath}")
        }
    }
}

tasks.matching {
    it.name == "assembleRelease" || it.name == "bundleRelease" ||
        it.name == "packageRelease" || it.name == "signReleaseBundle"
}.configureEach {
    dependsOn(validateReleaseSigning)
}

androidComponents {
    onVariants { variant ->
        variant.outputs.forEach { output ->
            val abi = output.filters.find { it.filterType == ABI }?.identifier
            output.outputFileName.set(
                output.versionName.map { versionName ->
                    if (abi == null) {
                        "${apkApplicationName}_$versionName.apk"
                    } else {
                        "${apkApplicationName}_${versionName}_$abi.apk"
                    }
                },
            )
        }
    }
}

tasks.register("verifyReleasePackaging") {
    group = "verification"
    description = "Checks ABI-split release APK legal assets and pure-Kotlin STCP packaging."
    dependsOn("assembleReproducibleRelease")
    doLast {
        val apkDirectory = layout.buildDirectory.dir("outputs/apk/reproducibleRelease").get().asFile
        val apks = apkDirectory.listFiles { file -> file.isFile && file.extension == "apk" }
            ?.sortedBy(File::getName)
            .orEmpty()
        val expectedNames = setOf(
            "${apkApplicationName}_${appVersionName}-repro_arm64-v8a.apk",
            "${apkApplicationName}_${appVersionName}-repro_armeabi-v7a.apk",
            "${apkApplicationName}_${appVersionName}-repro_x86_64.apk",
        )
        check(apks.map(File::getName).toSet() == expectedNames) {
            "Expected release APKs ${expectedNames.sorted()}, found ${apks.map(File::getName).sorted()}"
        }
        apks.forEach { apk ->
            ZipFile(apk).use { zip ->
                val names = zip.entries().asSequence().map { it.name }.toList()
                check("assets/licenses.txt" in names) { "Offline licenses are missing from ${apk.name}" }
                check(names.none { "gojni" in it.lowercase() || "gomobile" in it.lowercase() }) {
                    "GoMobile content must not be packaged in ${apk.name}"
                }
                val legalText = zip.getInputStream(zip.getEntry("assets/licenses.txt"))
                    .bufferedReader()
                    .use { it.readText() }
                listOf("PORTWEAVE LICENSE", "PORTWEAVE NOTICE", "THIRD-PARTY NOTICES", "OC Deck", "frp protocol")
                    .forEach { required -> check(required in legalText) { "${apk.name} offline legal bundle is missing: $required" } }
            }
        }
    }
}

kotlin { jvmToolchain(21) }

dependencies {
    implementation(project(":core"))
    implementation(project(":tunnel-engine"))

    implementation(libs.androidx.activity.compose)
    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.compose.material3)
    implementation(libs.androidx.compose.material.icons.extended)
    implementation(libs.androidx.compose.ui)
    implementation(libs.androidx.compose.ui.tooling.preview)
    implementation(libs.androidx.lifecycle.runtime.compose)
    implementation(libs.androidx.lifecycle.viewmodel.compose)
    implementation(libs.androidx.lifecycle.viewmodel.ktx)
    implementation(libs.androidx.navigation.compose)
    implementation(libs.kotlinx.coroutines.android)
    implementation(libs.androidx.room.runtime)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.zxing.android.embedded)

    debugImplementation(libs.androidx.compose.ui.tooling)
    debugImplementation(libs.androidx.compose.ui.test.manifest)
    testImplementation(libs.junit)
    testImplementation(libs.kotlinx.coroutines.test)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.compose.ui.test.junit4)
    androidTestImplementation(libs.androidx.test.ext.junit)
    androidTestImplementation(libs.androidx.test.runner)
}
