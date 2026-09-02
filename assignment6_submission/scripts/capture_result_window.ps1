param(
    [string]$Url = "http://localhost:8090/results/assignment6_alert_and_fallback.json",
    [string]$OutputPath = "assignment6_submission/evidence/07_alert_fallback_actual_result.png"
)

$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Drawing
Add-Type @"
using System;
using System.Runtime.InteropServices;

public static class Assignment6WindowCapture {
    [StructLayout(LayoutKind.Sequential)]
    public struct RECT {
        public int Left;
        public int Top;
        public int Right;
        public int Bottom;
    }

    [DllImport("user32.dll")]
    public static extern bool GetWindowRect(IntPtr hWnd, out RECT rect);

    [DllImport("user32.dll")]
    public static extern bool SetForegroundWindow(IntPtr hWnd);
}
"@

$chrome = Get-Command chrome.exe -ErrorAction SilentlyContinue
if (-not $chrome) {
    $candidate = "$env:ProgramFiles\Google\Chrome\Application\chrome.exe"
    if (-not (Test-Path -LiteralPath $candidate)) {
        $candidate = "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe"
    }
    if (-not (Test-Path -LiteralPath $candidate)) {
        throw "Chrome executable was not found."
    }
    $chromePath = $candidate
} else {
    $chromePath = $chrome.Source
}

$absoluteOutput = [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $OutputPath))
$outputDirectory = Split-Path -Parent $absoluteOutput
[System.IO.Directory]::CreateDirectory($outputDirectory) | Out-Null

Start-Process -FilePath $chromePath -ArgumentList @(
    "--new-window",
    "--window-size=1200,900",
    $Url
) | Out-Null

$deadline = (Get-Date).AddSeconds(15)
$window = $null
while ((Get-Date) -lt $deadline) {
    $window = Get-Process chrome -ErrorAction SilentlyContinue |
        Where-Object {
            $_.MainWindowHandle -ne 0 -and
            $_.MainWindowTitle -match "assignment6_alert_and_fallback|localhost:8090"
        } |
        Select-Object -First 1
    if ($window) {
        break
    }
    Start-Sleep -Milliseconds 500
}

if (-not $window) {
    throw "The assignment 6 result window was not found."
}

[Assignment6WindowCapture]::SetForegroundWindow($window.MainWindowHandle) | Out-Null
Start-Sleep -Seconds 2

$rect = New-Object Assignment6WindowCapture+RECT
if (-not [Assignment6WindowCapture]::GetWindowRect($window.MainWindowHandle, [ref]$rect)) {
    throw "Could not read the result window bounds."
}

$width = $rect.Right - $rect.Left
$height = $rect.Bottom - $rect.Top
if ($width -le 0 -or $height -le 0) {
    throw "The result window has invalid bounds: ${width}x${height}."
}

$bitmap = New-Object System.Drawing.Bitmap($width, $height)
$graphics = [System.Drawing.Graphics]::FromImage($bitmap)
try {
    $graphics.CopyFromScreen(
        (New-Object System.Drawing.Point($rect.Left, $rect.Top)),
        [System.Drawing.Point]::Empty,
        (New-Object System.Drawing.Size($width, $height))
    )
    $bitmap.Save($absoluteOutput, [System.Drawing.Imaging.ImageFormat]::Png)
} finally {
    $graphics.Dispose()
    $bitmap.Dispose()
}

Write-Output "CAPTURED=$absoluteOutput"
