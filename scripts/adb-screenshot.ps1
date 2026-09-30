[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

if (-not (Get-Command adb -ErrorAction SilentlyContinue)) {
    Write-Error "adb was not found. Add Android platform-tools to PATH and try again."
    exit 1
}

$device = (& adb devices) |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -match '^(\S+)\s+device$' } |
    Select-Object -First 1

if (-not $device) {
    Write-Error "No connected and authorized Android device was found."
    exit 1
}

$serial = ([regex]::Match($device, '^(\S+)')).Groups[1].Value
$projectRoot = Split-Path -Parent $PSScriptRoot
$tempDirectory = Join-Path $projectRoot "temp/screenshot"
$fileName = "screenshot_{0}.png" -f (Get-Date -Format "yyyyMMdd_HHmmss_fff")
$localPath = Join-Path $tempDirectory $fileName
$remotePath = "/data/local/tmp/portweave_screenshot_$PID.png"

New-Item -ItemType Directory -Path $tempDirectory -Force | Out-Null

try {
    & adb -s $serial shell screencap -p $remotePath
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to capture a screenshot on device '$serial'."
    }

    & adb -s $serial pull $remotePath $localPath
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to save the screenshot to '$localPath'."
    }
}
finally {
    & adb -s $serial shell rm -f $remotePath 2>$null | Out-Null
}

Write-Output "Screenshot saved: $localPath"
