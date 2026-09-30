[CmdletBinding()]
param(
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$gradleWrapper = Join-Path $projectRoot "gradlew.bat"
$outputDirectory = Join-Path $projectRoot "app/build/outputs/apk/release"

if (-not (Test-Path -LiteralPath $gradleWrapper -PathType Leaf)) {
    Write-Error "Gradle wrapper was not found: $gradleWrapper"
    exit 1
}

$gradleTasks = @()
if ($Clean) {
    $gradleTasks += "clean"
}
$gradleTasks += ":app:assembleRelease"

Push-Location $projectRoot
try {
    & $gradleWrapper @gradleTasks
    if ($LASTEXITCODE -ne 0) {
        throw "Release build failed with exit code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}

$artifacts = @(Get-ChildItem -LiteralPath $outputDirectory -Filter "*.apk" -File |
    Sort-Object Name)

if ($artifacts.Count -eq 0) {
    Write-Error "Release build completed, but no APK was found in '$outputDirectory'."
    exit 1
}

Write-Output "Release APKs:"
foreach ($artifact in $artifacts) {
    $hash = (Get-FileHash -LiteralPath $artifact.FullName -Algorithm SHA256).Hash
    Write-Output ("  {0} ({1:N2} MB)" -f $artifact.FullName, ($artifact.Length / 1MB))
    Write-Output "    SHA-256: $hash"
}
