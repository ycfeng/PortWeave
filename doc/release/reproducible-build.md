# 可复现构建

## 固定输入

- JDK 21、Gradle Wrapper 9.6.1、Android Gradle Plugin 9.2.1。
- Android SDK 36；依赖版本由 `gradle/libs.versions.toml` 固定。
- `VERSION_CODE=1`、`VERSION_NAME=0.1.0`，UTF-8 文件编码。
- 正式协议后端为纯 Kotlin；release 不依赖 Go、NDK 或本地原生制品。

## 验证方式

在 Windows PowerShell 中运行：

```powershell
.\tools\verify-reproducible-build.ps1
```

脚本在工作区 `.reproducible-build` 下创建两个不同且初始为空的临时 Gradle build root，禁用 build cache、并行执行和常驻 daemon，分别执行 `:app:assembleReproducibleRelease`。它比较 arm64-v8a、armeabi-v7a、x86_64 三个 unsigned、release-equivalent APK 的 SHA-256，并在任一不一致时失败；临时目录在结束时删除。正式 `assembleRelease` 保持 fail-closed，必须在受控环境提供发布签名；JKS 和密码不得进入仓库、Gradle 缓存或日志。

完整发布质量门禁：

```powershell
.\gradlew.bat releaseQualityCheck
```

## 证据边界

哈希一致证明相同固定输入在本机构建环境产生相同 unsigned APK，不证明源码来源、签名者身份、设备兼容性或远端协议兼容性。正式发布仍需执行 `doc/release/checklist.md` 的设备与签名项目。
