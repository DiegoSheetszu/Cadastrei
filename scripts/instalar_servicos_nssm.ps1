param(
    [string]$NssmPath = "C:\Tools\nssm\nssm.exe",
    [string]$BaseDir = "C:\Cadastrei",
    [ValidateSet("Homologacao", "Producao")]
    [string]$Ambiente = "Producao",
    [string]$ServicoMotoristas = "",
    [string]$ServicoAfastamentos = "",
    [string]$ServicoApi = "",
    [int]$IntervaloSegundos = 30,
    [int]$BatchSize = 100,
    [string]$DataInicioAfastamentos = "",
    [switch]$InstalarApi,
    [int]$ApiMaxTentativas = 10,
    [int]$ApiLockTimeoutMin = 15,
    [int]$ApiRetryBaseSec = 60,
    [int]$ApiRetryMaxSec = 3600,
    [switch]$Reinstalar
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $NssmPath)) {
    throw "nssm.exe nao encontrado em $NssmPath"
}

$logsDir = Join-Path $BaseDir "logs"
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null

if ($Ambiente -eq "Producao") {
    $exeMotoristas = Join-Path $BaseDir "apps\prod\CadastreiMotoristasProd\CadastreiMotoristasProd.exe"
    $exeAfastamentos = Join-Path $BaseDir "apps\prod\CadastreiAfastamentosProd\CadastreiAfastamentosProd.exe"
    $exeApi = Join-Path $BaseDir "apps\prod\CadastreiApiDispatchProd\CadastreiApiDispatchProd.exe"
} else {
    $exeMotoristas = Join-Path $BaseDir "apps\hom\CadastreiMotoristasHom\CadastreiMotoristasHom.exe"
    $exeAfastamentos = Join-Path $BaseDir "apps\hom\CadastreiAfastamentosHom\CadastreiAfastamentosHom.exe"
    $exeApi = Join-Path $BaseDir "apps\hom\CadastreiApiDispatchHom\CadastreiApiDispatchHom.exe"
}

if (-not (Test-Path $exeMotoristas)) {
    throw "Executavel de motoristas nao encontrado: $exeMotoristas"
}
if (-not (Test-Path $exeAfastamentos)) {
    throw "Executavel de afastamentos nao encontrado: $exeAfastamentos"
}
if ($InstalarApi -and -not (Test-Path $exeApi)) {
    throw "Executavel do despachante API nao encontrado: $exeApi"
}

if ([string]::IsNullOrWhiteSpace($ServicoMotoristas)) {
    if ($Ambiente -eq "Producao") { $ServicoMotoristas = "CadastreiMotoristasProd" }
    else { $ServicoMotoristas = "CadastreiMotoristasHom" }
}
if ([string]::IsNullOrWhiteSpace($ServicoAfastamentos)) {
    if ($Ambiente -eq "Producao") { $ServicoAfastamentos = "CadastreiAfastamentosProd" }
    else { $ServicoAfastamentos = "CadastreiAfastamentosHom" }
}
if ($InstalarApi -and [string]::IsNullOrWhiteSpace($ServicoApi)) {
    if ($Ambiente -eq "Producao") { $ServicoApi = "CadastreiApiDispatchProd" }
    else { $ServicoApi = "CadastreiApiDispatchHom" }
}

if ($Ambiente -eq "Producao") { $origemDb = "Vetorh_Prod" } else { $origemDb = "Vetorh_Hom" }

if ($Ambiente -eq "Producao") {
    $logMotoristas = "logs\motoristas_prod.log"
    $logAfastamentos = "logs\afastamentos_prod.log"
    $logApi = "logs\api_dispatch_prod.log"
} else {
    $logMotoristas = "logs\motoristas_hom.log"
    $logAfastamentos = "logs\afastamentos_hom.log"
    $logApi = "logs\api_dispatch_hom.log"
}

$argsMotoristas = "--origem-db $origemDb --destino-db Cadastrei --intervalo $IntervaloSegundos --batch-size $BatchSize --log-file $logMotoristas"
$argsAfastamentos = "--origem-db $origemDb --destino-db Cadastrei --intervalo $IntervaloSegundos --batch-size $BatchSize --log-file $logAfastamentos"
if (-not [string]::IsNullOrWhiteSpace($DataInicioAfastamentos)) {
    $argsAfastamentos = "$argsAfastamentos --data-inicio $DataInicioAfastamentos"
}
$argsApi = "--destino-db Cadastrei --schema-destino dbo --intervalo $IntervaloSegundos --batch-motoristas $BatchSize --batch-afastamentos $BatchSize --max-tentativas $ApiMaxTentativas --lock-timeout-min $ApiLockTimeoutMin --retry-base-sec $ApiRetryBaseSec --retry-max-sec $ApiRetryMaxSec --log-file $logApi"

function Test-ServiceExists([string]$Name) {
    sc.exe query $Name | Out-Null
    return ($LASTEXITCODE -eq 0)
}

function Configure-Service([string]$Name, [string]$Exe, [string]$Args, [string]$StdoutLog, [string]$StderrLog) {
    $exists = Test-ServiceExists -Name $Name
    if ($exists -and $Reinstalar) {
        & $NssmPath stop $Name | Out-Null
        & $NssmPath remove $Name confirm | Out-Null
        $exists = $false
    }

    if (-not $exists) {
        & $NssmPath install $Name $Exe $Args | Out-Null
    } else {
        & $NssmPath set $Name Application $Exe | Out-Null
        & $NssmPath set $Name AppParameters $Args | Out-Null
    }

    & $NssmPath set $Name AppDirectory $BaseDir | Out-Null
    & $NssmPath set $Name Start SERVICE_AUTO_START | Out-Null
    & $NssmPath set $Name AppExit Default Restart | Out-Null
    & $NssmPath set $Name AppRestartDelay 5000 | Out-Null
    & $NssmPath set $Name AppStdout $StdoutLog | Out-Null
    & $NssmPath set $Name AppStderr $StderrLog | Out-Null
}

function Get-ServiceState([string]$Name) {
    $raw = sc.exe query $Name 2>&1
    if ($LASTEXITCODE -ne 0) {
        return "NOT_FOUND"
    }

    $stateLine = ($raw | Select-String -Pattern "STATE").Line
    if (-not $stateLine) {
        return "UNKNOWN"
    }

    if ($stateLine -match "RUNNING") { return "RUNNING" }
    if ($stateLine -match "STOPPED") { return "STOPPED" }
    if ($stateLine -match "PAUSED") { return "PAUSED" }
    if ($stateLine -match "START_PENDING") { return "START_PENDING" }
    if ($stateLine -match "STOP_PENDING") { return "STOP_PENDING" }
    return "UNKNOWN"
}

function Start-ServiceChecked([string]$Name, [string]$StdoutLog, [string]$StderrLog) {
    & $NssmPath start $Name | Out-Null
    Start-Sleep -Seconds 1

    $maxChecks = 20
    for ($i = 0; $i -lt $maxChecks; $i++) {
        $state = Get-ServiceState $Name
        if ($state -eq "RUNNING") {
            Write-Host "$Name iniciado com sucesso."
            return
        }
        if ($state -eq "PAUSED" -or $state -eq "STOPPED") {
            throw @"
Falha ao iniciar o servico $Name. Estado atual: $state.
Verifique os logs:
- $StdoutLog
- $StderrLog
"@
        }
        Start-Sleep -Seconds 1
    }

    $stateFinal = Get-ServiceState $Name
    throw @"
Falha ao iniciar o servico $Name. Timeout aguardando RUNNING (estado final: $stateFinal).
Verifique os logs:
- $StdoutLog
- $StderrLog
"@
}

$stdoutMotoristas = Join-Path $logsDir "motoristas_nssm.out.log"
$stderrMotoristas = Join-Path $logsDir "motoristas_nssm.err.log"
$stdoutAfastamentos = Join-Path $logsDir "afastamentos_nssm.out.log"
$stderrAfastamentos = Join-Path $logsDir "afastamentos_nssm.err.log"
$stdoutApi = Join-Path $logsDir "api_dispatch_nssm.out.log"
$stderrApi = Join-Path $logsDir "api_dispatch_nssm.err.log"

Configure-Service `
    -Name $ServicoMotoristas `
    -Exe $exeMotoristas `
    -Args $argsMotoristas `
    -StdoutLog $stdoutMotoristas `
    -StderrLog $stderrMotoristas

Configure-Service `
    -Name $ServicoAfastamentos `
    -Exe $exeAfastamentos `
    -Args $argsAfastamentos `
    -StdoutLog $stdoutAfastamentos `
    -StderrLog $stderrAfastamentos

if ($InstalarApi) {
    Configure-Service `
        -Name $ServicoApi `
        -Exe $exeApi `
        -Args $argsApi `
        -StdoutLog $stdoutApi `
        -StderrLog $stderrApi
}

Start-ServiceChecked -Name $ServicoMotoristas -StdoutLog $stdoutMotoristas -StderrLog $stderrMotoristas
Start-ServiceChecked -Name $ServicoAfastamentos -StdoutLog $stdoutAfastamentos -StderrLog $stderrAfastamentos
if ($InstalarApi) {
    Start-ServiceChecked -Name $ServicoApi -StdoutLog $stdoutApi -StderrLog $stderrApi
}

Write-Host "Servicos configurados e iniciados com sucesso."
Write-Host "Motoristas:   $ServicoMotoristas"
Write-Host "Afastamentos: $ServicoAfastamentos"
if ($InstalarApi) {
    Write-Host "Despachante API: $ServicoApi"
}
Write-Host "Ambiente:     $Ambiente (origem=$origemDb)"
Write-Host "Base:         $BaseDir"
