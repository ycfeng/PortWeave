plugins {
    alias(libs.plugins.android.library)
}

android {
    namespace = "dev.portweave.testsupport"
    compileSdk = 36
    defaultConfig { minSdk = 26 }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }
}

kotlin { jvmToolchain(21) }

dependencies {
    api(project(":core"))
    api(libs.kotlinx.coroutines.test)
}
