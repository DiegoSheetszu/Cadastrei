param(
    [string]$NssmPath = "C:\Tools\nssm\nssm.exe",
    [string]$BaseDir = "C:\Cadastrei",
    [ValidateSet("Homologacao", "Producao")]
    [string]$Ambiente = "Producao",
    [string]$ServicoMotoristas = "",
    [string]$ServicoAfastamentos = "",
    [string]$ServicoApiMotoristas = "",
    [string]$ServicoApiAfastamentos = "",
    [int]$IntervaloSegundos = 30,
    [int]$BatchSize = 100,
    [string]$DataInicioAfastamentos = "",
    [switch]$InstalarApiMotoristas,
    [switch]$InstalarApiAfastamentos,
    [switch]$InstalarApi,
    [int]$ApiMaxTentativas = 10,
    [int]$ApiLockTimeoutMin = 15,
    [int]$ApiRetryBaseSec = 60,
    [int]$ApiRetryMaxSec = 3600,
    [string]$ClienteApiId = "",
    [string]$ApiEndpointIdMotoristas = "",
    [string]$ApiEndpointIdAfastamentos = "",
    [string]$ApiRegistryFile = "",
    [switch]$SemRegistryApi,
    [switch]$Reinstalar,
    [switch]$ApiServicosUnicos,
    [switch]$SomenteApi
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $NssmPath)) {
    throw "nssm.exe nao encontrado em $NssmPath"
}

$logsDir = Join-Path $BaseDir "logs"
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null

$instalarApiM = [bool]$InstalarApiMotoristas -or [bool]$InstalarApi
$instalarApiA = [bool]$InstalarApiAfastamentos -or [bool]$InstalarApi
$instalarSync = -not [bool]$SomenteApi

if (-not $instalarSync -and -not $instalarApiM -and -not $instalarApiA) {
    throw "Nenhum servico selecionado. Use -InstalarApiMotoristas/-InstalarApiAfastamentos/-InstalarApi junto de -SomenteApi."
}

if ($Ambiente -eq "Producao") {
    $exeMotoristas = Join-Path $BaseDir "apps\prod\CadastreiMotoristasProd\CadastreiMotoristasProd.exe"
    $exeAfastamentos = Join-Path $BaseDir "apps\prod\CadastreiAfastamentosProd\CadastreiAfastamentosProd.exe"
    $exeApiMotoristas = Join-Path $BaseDir "apps\prod\CadastreiApiMotoristasProd\CadastreiApiMotoristasProd.exe"
    $exeApiAfastamentos = Join-Path $BaseDir "apps\prod\CadastreiApiAfastamentosProd\CadastreiApiAfastamentosProd.exe"
} else {
    $exeMotoristas = Join-Path $BaseDir "apps\hom\CadastreiMotoristasHom\CadastreiMotoristasHom.exe"
    $exeAfastamentos = Join-Path $BaseDir "apps\hom\CadastreiAfastamentosHom\CadastreiAfastamentosHom.exe"
    $exeApiMotoristas = Join-Path $BaseDir "apps\hom\CadastreiApiMotoristasHom\CadastreiApiMotoristasHom.exe"
    $exeApiAfastamentos = Join-Path $BaseDir "apps\hom\CadastreiApiAfastamentosHom\CadastreiApiAfastamentosHom.exe"
}

if ($instalarSync -and -not (Test-Path $exeMotoristas)) {
    throw "Executavel de motoristas nao encontrado: $exeMotoristas"
}
if ($instalarSync -and -not (Test-Path $exeAfastamentos)) {
    throw "Executavel de afastamentos nao encontrado: $exeAfastamentos"
}
if ($instalarApiM -and -not (Test-Path $exeApiMotoristas)) {
    throw "Executavel do despachante API de motoristas nao encontrado: $exeApiMotoristas"
}
if ($instalarApiA -and -not (Test-Path $exeApiAfastamentos)) {
    throw "Executavel do despachante API de afastamentos nao encontrado: $exeApiAfastamentos"
}

if ($instalarSync -and [string]::IsNullOrWhiteSpace($ServicoMotoristas)) {
    if ($Ambiente -eq "Producao") { $ServicoMotoristas = "CadastreiMotoristasProd" }
    else { $ServicoMotoristas = "CadastreiMotoristasHom" }
}
if ($instalarSync -and [string]::IsNullOrWhiteSpace($ServicoAfastamentos)) {
    if ($Ambiente -eq "Producao") { $ServicoAfastamentos = "CadastreiAfastamentosProd" }
    else { $ServicoAfastamentos = "CadastreiAfastamentosHom" }
}
if ($instalarApiM -and [string]::IsNullOrWhiteSpace($ServicoApiMotoristas)) {
    if ($ApiServicosUnicos) { $ServicoApiMotoristas = "CadastreiApiMotoristas" }
    elseif ($Ambiente -eq "Producao") { $ServicoApiMotoristas = "CadastreiApiMotoristasProd" }
    else { $ServicoApiMotoristas = "CadastreiApiMotoristasHom" }
}
if ($instalarApiA -and [string]::IsNullOrWhiteSpace($ServicoApiAfastamentos)) {
    if ($ApiServicosUnicos) { $ServicoApiAfastamentos = "CadastreiApiAfastamentos" }
    elseif ($Ambiente -eq "Producao") { $ServicoApiAfastamentos = "CadastreiApiAfastamentosProd" }
    else { $ServicoApiAfastamentos = "CadastreiApiAfastamentosHom" }
}

if ($Ambiente -eq "Producao") { $origemDb = "Vetorh_Prod" } else { $origemDb = "Vetorh_Hom" }

if ($Ambiente -eq "Producao") {
    $logMotoristas = "logs\motoristas_prod.log"
    $logAfastamentos = "logs\afastamentos_prod.log"
    $logApiMotoristas = "logs\api_motoristas_prod.log"
    $logApiAfastamentos = "logs\api_afastamentos_prod.log"
} else {
    $logMotoristas = "logs\motoristas_hom.log"
    $logAfastamentos = "logs\afastamentos_hom.log"
    $logApiMotoristas = "logs\api_motoristas_hom.log"
    $logApiAfastamentos = "logs\api_afastamentos_hom.log"
}
if ($ApiServicosUnicos) {
    $logApiMotoristas = "logs\api_motoristas.log"
    $logApiAfastamentos = "logs\api_afastamentos.log"
}

$argsMotoristas = "--origem-db $origemDb --destino-db Cadastrei --intervalo $IntervaloSegundos --batch-size $BatchSize --log-file $logMotoristas"
$argsAfastamentos = "--origem-db $origemDb --destino-db Cadastrei --intervalo $IntervaloSegundos --batch-size $BatchSize --log-file $logAfastamentos"
if (-not [string]::IsNullOrWhiteSpace($DataInicioAfastamentos)) {
    $argsAfastamentos = "$argsAfastamentos --data-inicio $DataInicioAfastamentos"
}
$argsApiMotoristas = "--destino-db Cadastrei --schema-destino dbo --intervalo $IntervaloSegundos --batch-motoristas $BatchSize --max-tentativas $ApiMaxTentativas --lock-timeout-min $ApiLockTimeoutMin --retry-base-sec $ApiRetryBaseSec --retry-max-sec $ApiRetryMaxSec --log-file $logApiMotoristas"
$argsApiAfastamentos = "--destino-db Cadastrei --schema-destino dbo --intervalo $IntervaloSegundos --batch-afastamentos $BatchSize --max-tentativas $ApiMaxTentativas --lock-timeout-min $ApiLockTimeoutMin --retry-base-sec $ApiRetryBaseSec --retry-max-sec $ApiRetryMaxSec --log-file $logApiAfastamentos"

if (-not [string]::IsNullOrWhiteSpace($ClienteApiId)) {
    $argsApiMotoristas = "$argsApiMotoristas --cliente-id $ClienteApiId"
    $argsApiAfastamentos = "$argsApiAfastamentos --cliente-id $ClienteApiId"
}
if (-not [string]::IsNullOrWhiteSpace($ApiEndpointIdMotoristas)) {
    $argsApiMotoristas = "$argsApiMotoristas --endpoint-id $ApiEndpointIdMotoristas"
}
if (-not [string]::IsNullOrWhiteSpace($ApiEndpointIdAfastamentos)) {
    $argsApiAfastamentos = "$argsApiAfastamentos --endpoint-id $ApiEndpointIdAfastamentos"
}
if (-not [string]::IsNullOrWhiteSpace($ApiRegistryFile)) {
    $argsApiMotoristas = "$argsApiMotoristas --registry-file `"$ApiRegistryFile`""
    $argsApiAfastamentos = "$argsApiAfastamentos --registry-file `"$ApiRegistryFile`""
}
if ($SemRegistryApi) {
    $argsApiMotoristas = "$argsApiMotoristas --sem-registry"
    $argsApiAfastamentos = "$argsApiAfastamentos --sem-registry"
}

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
$stdoutApiMotoristas = Join-Path $logsDir "api_motoristas_nssm.out.log"
$stderrApiMotoristas = Join-Path $logsDir "api_motoristas_nssm.err.log"
$stdoutApiAfastamentos = Join-Path $logsDir "api_afastamentos_nssm.out.log"
$stderrApiAfastamentos = Join-Path $logsDir "api_afastamentos_nssm.err.log"

if ($instalarSync) {
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
}

if ($instalarApiM) {
    Configure-Service `
        -Name $ServicoApiMotoristas `
        -Exe $exeApiMotoristas `
        -Args $argsApiMotoristas `
        -StdoutLog $stdoutApiMotoristas `
        -StderrLog $stderrApiMotoristas
}

if ($instalarApiA) {
    Configure-Service `
        -Name $ServicoApiAfastamentos `
        -Exe $exeApiAfastamentos `
        -Args $argsApiAfastamentos `
        -StdoutLog $stdoutApiAfastamentos `
        -StderrLog $stderrApiAfastamentos
}

if ($instalarSync) {
    Start-ServiceChecked -Name $ServicoMotoristas -StdoutLog $stdoutMotoristas -StderrLog $stderrMotoristas
    Start-ServiceChecked -Name $ServicoAfastamentos -StdoutLog $stdoutAfastamentos -StderrLog $stderrAfastamentos
}
if ($instalarApiM) {
    Start-ServiceChecked -Name $ServicoApiMotoristas -StdoutLog $stdoutApiMotoristas -StderrLog $stderrApiMotoristas
}
if ($instalarApiA) {
    Start-ServiceChecked -Name $ServicoApiAfastamentos -StdoutLog $stdoutApiAfastamentos -StderrLog $stderrApiAfastamentos
}

Write-Host "Servicos configurados e iniciados com sucesso."
if ($instalarSync) {
    Write-Host "Motoristas:   $ServicoMotoristas"
    Write-Host "Afastamentos: $ServicoAfastamentos"
}
if ($instalarApiM) {
    Write-Host "API Motoristas: $ServicoApiMotoristas"
}
if ($instalarApiA) {
    Write-Host "API Afastamentos: $ServicoApiAfastamentos"
}
Write-Host "Ambiente:     $Ambiente (origem=$origemDb)"
Write-Host "Base:         $BaseDir"
