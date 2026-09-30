plugins {
    alias(libs.plugins.android.library)
}

android {
    namespace = "dev.portweave.tunnel.engine"
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
    api(project(":frpc-stcp-visitor"))
    implementation(libs.bcprov)
    implementation(libs.jsch)
    implementation(libs.kotlinx.coroutines.android)

    testImplementation(libs.junit)
    testImplementation(libs.bcpkix)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.sshd.common)
    testImplementation(libs.sshd.core)
}
