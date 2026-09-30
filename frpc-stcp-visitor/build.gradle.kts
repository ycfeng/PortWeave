import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.testing.Test

plugins {
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlin.serialization)
}

android {
    namespace = "dev.portweave.frpc.stcp"
    compileSdk = 36
    defaultConfig { minSdk = 26 }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }
}

kotlin { jvmToolchain(21) }

dependencies {
    implementation(libs.bcprov)
    implementation(libs.kotlinx.coroutines.android)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(libs.junit)
    testImplementation(libs.kotlinx.coroutines.test)
}

tasks.register<JavaExec>("frpcInteropTest") {
    group = "verification"
    description = "Runs the pinned official frp 0.69.1 interoperability suite."
    dependsOn(
        "compileDebugUnitTestKotlin",
        "compileDebugUnitTestJavaWithJavac",
        "processDebugUnitTestJavaRes",
    )
    mainClass.set("io.github.ycfeng.ocdeck.frpcstcpvisitor.interop.FrpcInteropHarness")
    classpath = tasks.named<Test>("testDebugUnitTest").get().classpath
    workingDir(rootProject.projectDir)
    systemProperty(
        "ocdeck.frp.interop.cacheDir",
        gradle.gradleUserHomeDir.resolve("caches/portweave/frp-interop/v0.69.1").absolutePath,
    )
    jvmArgs("-Dfile.encoding=UTF-8")
    maxHeapSize = "512m"
}
