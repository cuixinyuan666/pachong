#Requires -Version 5.1
<#
.SYNOPSIS
  从 GitHub 云仓库一键部署 MarketPulse（Rust GUI exe + Selenium 备用脚本）。

.DESCRIPTION
  默认从 Releases 下载最新版预编译 exe，无需本机安装 Rust。
  加 -BuildFromSource 则在已 clone 的仓库里 cargo build --release。

.EXAMPLE
  git clone https://github.com/cuixinyuan666/pachong.git
  cd pachong
  .\scripts\deploy-marketpulse.ps1

.EXAMPLE
  .\scripts\deploy-marketpulse.ps1 -InstallDir "D:\MarketPulse" -Tag v1.10.2 -Force
#>
param(
    [string]$InstallDir = "",
    [string]$Tag = "latest",
    [switch]$BuildFromSource,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$RepoOwner = "cuixinyuan666"
$RepoName = "pachong"

if ([string]::IsNullOrWhiteSpace($InstallDir)) {
    $InstallDir = Join-Path (Get-Location) "MarketPulse"
}
$InstallDir = [System.IO.Path]::GetFullPath($InstallDir)
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null

function Get-ReleaseJson {
    param([string]$TagName)
    $base = "https://api.github.com/repos/$RepoOwner/$RepoName/releases"
    $headers = @{ "User-Agent" = "MarketPulse-Deploy" }
    if ($TagName -eq "latest") {
        return Invoke-RestMethod -Uri "$base/latest" -Headers $headers
    }
    return Invoke-RestMethod -Uri "$base/tags/$TagName" -Headers $headers
}

function Save-Asset {
    param(
        [string]$Url,
        [string]$Dest
    )
    Write-Host "  -> $Dest"
    Invoke-WebRequest -Uri $Url -OutFile $Dest -UseBasicParsing
}

$repoRoot = Split-Path $PSScriptRoot -Parent

if ($BuildFromSource) {
    Write-Host "模式: 源码编译 (cargo build --release)"
    $cargoDir = Join-Path $repoRoot "rust_crawler"
    if (-not (Test-Path $cargoDir)) {
        $msg = "找不到 rust_crawler 目录。请先 git clone 仓库: https://github.com/$RepoOwner/$RepoName.git"
        throw $msg
    }
    $cargo = Get-Command cargo -ErrorAction SilentlyContinue
    if (-not $cargo) {
        throw "未检测到 cargo。请安装 Rust: https://rustup.rs  或去掉 -BuildFromSource 直接下载 Release exe。"
    }
    Push-Location $cargoDir
    try {
        & cargo build --release
        if ($LASTEXITCODE -ne 0) {
            $code = $LASTEXITCODE
            throw "cargo build 失败，退出码: $code"
        }
        $built = Join-Path $cargoDir "target\release\baidu_finance_rust.exe"
        if (-not (Test-Path $built)) {
            throw "未找到编译产物: $built"
        }
        Copy-Item $built (Join-Path $InstallDir "baidu_finance_rust.exe") -Force
        $py = Join-Path $repoRoot "baidu_selenium_fallback.py"
        if (Test-Path $py) {
            Copy-Item $py $InstallDir -Force
        }
    } finally {
        Pop-Location
    }
} else {
    Write-Host "模式: 下载 GitHub Release ($Tag)"
    $release = Get-ReleaseJson -TagName $Tag
    Write-Host "版本: $($release.tag_name)"
    $wanted = @(
        @{ Pattern = "baidu_finance_rust"; Out = "baidu_finance_rust.exe" },
        @{ Pattern = "baidu_selenium_fallback.py"; Out = "baidu_selenium_fallback.py" }
    )
    foreach ($w in $wanted) {
        $asset = $release.assets | Where-Object { $_.name -like "*$($w.Pattern)*" } | Select-Object -First 1
        if (-not $asset) {
            Write-Warning "Release 中未找到: $($w.Pattern)"
            continue
        }
        $dest = Join-Path $InstallDir $w.Out
        if ((Test-Path $dest) -and -not $Force) {
            Write-Host "已存在，跳过: $($w.Out)  (加 -Force 覆盖)"
            continue
        }
        Save-Asset -Url $asset.browser_download_url -Dest $dest
    }
}

New-Item -ItemType Directory -Force -Path (Join-Path $InstallDir "logs") | Out-Null

Write-Host ""
Write-Host "========================================"
Write-Host " MarketPulse 部署完成"
Write-Host " 目录: $InstallDir"
Write-Host " 启动: 双击 baidu_finance_rust.exe"
Write-Host " 数据库 market_data.db 首次抓取后自动生成于同目录"
Write-Host "========================================"
