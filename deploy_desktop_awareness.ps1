param(
    [string]$PluginsRoot = "C:\Users\Administrator\Downloads\AstrBot\data\plugins",
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-RelativePathCompat {
    param([string]$BasePath, [string]$TargetPath)
    $baseUri = New-Object System.Uri(($BasePath.TrimEnd('\') + '\'))
    $targetUri = New-Object System.Uri($TargetPath)
    return [System.Uri]::UnescapeDataString(
        $baseUri.MakeRelativeUri($targetUri).ToString()
    ).Replace('/', '\')
}

function Should-SkipPath {
    param([string]$RelativePath)
    $segments = $RelativePath -split "[\\/]+"
    if ($segments -contains ".git" -or
        $segments -contains "__pycache__" -or
        $segments -contains ".pytest_cache" -or
        $segments -contains ".ruff_cache" -or
        $segments -contains ".venv" -or
        $segments -contains "tests") {
        return $true
    }
    return ([IO.Path]::GetExtension($RelativePath) -eq ".pyc")
}

function Invoke-DeployAction {
    param([string]$Description, [scriptblock]$Action)
    if ($DryRun) {
        Write-Host "[DryRun] $Description"
        return
    }
    Write-Host $Description
    & $Action
}

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$MetadataPath = Join-Path $RepoRoot "metadata.yaml"
$PluginName = "astrbot_plugin_ag99_desktop_awareness"
if (-not (Test-Path -LiteralPath $MetadataPath -PathType Leaf)) {
    throw "metadata.yaml not found: $MetadataPath"
}

$ResolvedPluginsRoot = (Resolve-Path -LiteralPath $PluginsRoot).Path
$TargetPluginRoot = [IO.Path]::GetFullPath((Join-Path $ResolvedPluginsRoot $PluginName))
if (-not $TargetPluginRoot.StartsWith($ResolvedPluginsRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Resolved target path escapes plugins root: $TargetPluginRoot"
}

$managedFiles = [System.Collections.Generic.HashSet[string]]::new(
    [StringComparer]::OrdinalIgnoreCase
)
Invoke-DeployAction "Ensure target directory: $TargetPluginRoot" {
    New-Item -ItemType Directory -Path $TargetPluginRoot -Force | Out-Null
}

$sourceFiles = Get-ChildItem -LiteralPath $RepoRoot -Recurse -File | Where-Object {
    $relative = Get-RelativePathCompat -BasePath $RepoRoot -TargetPath $_.FullName
    (-not (Should-SkipPath $relative)) -and ($_.Name -ne "deploy_desktop_awareness.ps1")
}
foreach ($file in $sourceFiles) {
    $relative = Get-RelativePathCompat -BasePath $RepoRoot -TargetPath $file.FullName
    [void]$managedFiles.Add($relative)
    $targetPath = Join-Path $TargetPluginRoot $relative
    Invoke-DeployAction "Copy file: $relative" {
        New-Item -ItemType Directory -Path (Split-Path -Parent $targetPath) -Force | Out-Null
        Copy-Item -LiteralPath $file.FullName -Destination $targetPath -Force
    }
}

if (Test-Path -LiteralPath $TargetPluginRoot -PathType Container) {
    $targetFiles = Get-ChildItem -LiteralPath $TargetPluginRoot -Recurse -File
    foreach ($file in $targetFiles) {
        $relative = Get-RelativePathCompat -BasePath $TargetPluginRoot -TargetPath $file.FullName
        if ((Should-SkipPath $relative) -or $managedFiles.Contains($relative)) {
            continue
        }
        Invoke-DeployAction "Remove stale file: $relative" {
            Remove-Item -LiteralPath $file.FullName -Force
        }
    }
}

Write-Host "Deploy source : $RepoRoot"
Write-Host "Deploy target : $TargetPluginRoot"
Write-Host "Plugin name   : $PluginName"
