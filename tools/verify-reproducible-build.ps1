$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$temporaryRoot = Join-Path $projectRoot (".reproducible-build\" + [guid]::NewGuid().ToString("N"))
$temporaryRoot = [System.IO.Path]::GetFullPath($temporaryRoot)
$isolatedBuildParent = [System.IO.Path]::GetFullPath((Join-Path $projectRoot ".reproducible-build"))
if (-not $temporaryRoot.StartsWith($isolatedBuildParent + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Temporary directory resolved outside the isolated build directory: $temporaryRoot"
}

New-Item -ItemType Directory -Path $temporaryRoot | Out-Null
$firstBuildRoot = Join-Path $temporaryRoot "first"
$secondBuildRoot = Join-Path $temporaryRoot "second"
$initScript = Join-Path $projectRoot "tools\isolated-build-root.gradle"
$previousBuildRoot = $env:PORTWEAVE_ISOLATED_BUILD_ROOT
try {
    New-Item -ItemType Directory -Path $firstBuildRoot | Out-Null
    $env:PORTWEAVE_ISOLATED_BUILD_ROOT = $firstBuildRoot
    & (Join-Path $projectRoot "gradlew.bat") --no-build-cache --no-parallel --no-daemon `
        --init-script $initScript :app:assembleReproducibleRelease
    if ($LASTEXITCODE -ne 0) { throw "First release build failed" }
    $firstApkDirectory = Join-Path $firstBuildRoot "app\outputs\apk\reproducibleRelease"
    $firstApks = @(Get-ChildItem -LiteralPath $firstApkDirectory -Filter "*.apk" | Sort-Object Name)
    if ($firstApks.Count -ne 3) { throw "Expected three ABI APKs, found $($firstApks.Count)" }
    $firstHashes = @{}
    foreach ($apk in $firstApks) {
        $firstHashes[$apk.Name] = (Get-FileHash -LiteralPath $apk.FullName -Algorithm SHA256).Hash
    }

    New-Item -ItemType Directory -Path $secondBuildRoot | Out-Null
    $env:PORTWEAVE_ISOLATED_BUILD_ROOT = $secondBuildRoot
    & (Join-Path $projectRoot "gradlew.bat") --no-build-cache --no-parallel --no-daemon `
        --init-script $initScript :app:assembleReproducibleRelease
    if ($LASTEXITCODE -ne 0) { throw "Second release build failed" }
    $secondApkDirectory = Join-Path $secondBuildRoot "app\outputs\apk\reproducibleRelease"
    $secondApks = @(Get-ChildItem -LiteralPath $secondApkDirectory -Filter "*.apk" | Sort-Object Name)
    if ($secondApks.Count -ne 3) { throw "Expected three ABI APKs, found $($secondApks.Count)" }
    foreach ($apk in $secondApks) {
        $secondHash = (Get-FileHash -LiteralPath $apk.FullName -Algorithm SHA256).Hash
        $firstHash = $firstHashes[$apk.Name]
        Write-Output "$($apk.Name) first=$firstHash"
        Write-Output "$($apk.Name) second=$secondHash"
        if ($firstHash -ne $secondHash) {
            throw "$($apk.Name) is not reproducible for the current fixed inputs"
        }
    }
    Write-Output "Reproducible unsigned release-equivalent ABI APKs verified."
}
finally {
    $env:PORTWEAVE_ISOLATED_BUILD_ROOT = $previousBuildRoot
    if (Test-Path -LiteralPath $temporaryRoot) {
        Remove-Item -LiteralPath $temporaryRoot -Recurse -Force
    }
}
