param([switch]$CheckOnly)

$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$profilePath = [IO.Path]::GetFullPath((Join-Path $projectRoot '.local/linkedin-test-browser'))
$projectPrefix = $projectRoot.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
if (-not $profilePath.StartsWith($projectPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'The test profile must remain inside the LinkedIn-OS project.'
}

$browserCandidates = @(
    'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe'
)
$browserPath = $browserCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $browserPath) { throw 'Installed Chrome was not found. No browser was installed or changed.' }

if ($CheckOnly) {
    Write-Output "Chrome is available. Test profile will be $profilePath"
    Write-Output 'No profile was created and no browser was launched.'
    return
}

New-Item -ItemType Directory -Path $profilePath -Force | Out-Null
$browserArguments = @(
    ('--user-data-dir="{0}"' -f $profilePath),
    '--no-first-run',
    '--no-default-browser-check',
    'https://www.linkedin.com/login'
)
# The operator explicitly runs this helper to use a visible login window.
Start-Process -FilePath $browserPath -ArgumentList $browserArguments -WindowStyle Normal | Out-Null
Write-Output 'Sign in with a dedicated LinkedIn test account in the separate Chrome window.'
Write-Output 'Close that test browser after login, then tell Codex it is ready.'
Write-Output 'Do not share passwords, cookies, tokens or network captures. No login is observed by this helper.'
