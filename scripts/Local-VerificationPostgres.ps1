param([switch]$Stop)

$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$dataPath = [IO.Path]::GetFullPath((Join-Path $projectRoot '.local/verification-postgres'))
$expectedParent = [IO.Path]::GetFullPath((Join-Path $projectRoot '.local')) + [IO.Path]::DirectorySeparatorChar
if (-not $dataPath.StartsWith($expectedParent, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'PostgreSQL data must remain inside this project.'
}
$binPath = 'C:/Program Files/PostgreSQL/17/bin'
$controlPath = Join-Path $binPath 'pg_ctl.exe'
$initPath = Join-Path $binPath 'initdb.exe'
if (-not (Test-Path -LiteralPath $controlPath) -or -not (Test-Path -LiteralPath $initPath)) {
    throw 'The already installed PostgreSQL 17 binaries were not found.'
}

if ($Stop) {
    if (Test-Path -LiteralPath (Join-Path $dataPath 'postmaster.pid')) {
        & $controlPath stop -D $dataPath -m fast -w
        if ($LASTEXITCODE -ne 0) { throw 'Stopping the project cluster failed.' }
    }
    exit
}

New-Item -ItemType Directory -Force -Path $expectedParent | Out-Null
if (-not (Test-Path -LiteralPath (Join-Path $dataPath 'PG_VERSION'))) {
    if (Test-Path -LiteralPath $dataPath) { throw 'Existing unrecognized data directory will not be overwritten.' }
    & $initPath -D $dataPath -U nos_test -A trust --encoding=UTF8 --no-locale *> (Join-Path $expectedParent 'postgres-init.log')
    if ($LASTEXITCODE -ne 0) { throw 'Project-local initdb failed; inspect .local/postgres-init.log.' }
}
if (-not (Test-Path -LiteralPath (Join-Path $dataPath 'postmaster.pid'))) {
    $arguments = @('start', '-D', $dataPath, '-l', (Join-Path $expectedParent 'postgres-server.log'), '-o', '"-p 15432 -h 127.0.0.1"', '-w')
    $process = Start-Process -FilePath $controlPath -ArgumentList $arguments -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $expectedParent 'postgres-start.log') `
        -RedirectStandardError (Join-Path $expectedParent 'postgres-start-error.log')
    # Start-Process -Wait also waits for the long-lived PostgreSQL descendants.
    # Wait only for pg_ctl, which reports startup success and then exits.
    if (-not $process.WaitForExit(30000)) { throw 'pg_ctl startup did not finish within 30 seconds.' }
    $process.Refresh()
    if ($process.ExitCode -ne 0) { throw 'Project-local PostgreSQL start failed; inspect .local/postgres-start*.log.' }
}
Write-Output 'Project verification PostgreSQL: 127.0.0.1:15432, user nos_test, database postgres. Stop with -Stop.'
