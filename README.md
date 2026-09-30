# PortWeave Android

PortWeave is an Android TCP port-mapping manager with SSH local forwarding and a pure-Kotlin frpc STCP visitor. It provides dependency-aware orchestration, loopback-only listeners, Android Keystore-backed credentials, foreground recovery, structured diagnostics, adaptive Compose UI, and offline legal notices.

## Build

Use JDK 21 and Android SDK 36:

```powershell
.\gradlew.bat releaseQualityCheck
```

Release preparation and evidence boundaries are documented in [doc/release/checklist.md](doc/release/checklist.md) and [doc/release/reproducible-build.md](doc/release/reproducible-build.md). Privacy behavior is documented in [PRIVACY.md](PRIVACY.md).
