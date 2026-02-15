param(
    [string]$NssmPath = "C:\Tools\nssm\nssm.exe",
    [string]$BaseDir = "C:\Cadastrei",
    [ValidateSet("Homologacao", "Producao")]
    [string]$Ambiente = "Producao",
    [string]$ServicoMotoristas = "",
    [string]$ServicoAfastamentos = "",
    [int]$IntervaloSegundos = 30,
    [int]$BatchSize = 100,
    [string]$DataInicioAfastamentos = "",
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
} else {
    $exeMotoristas = Join-Path $BaseDir "apps\hom\CadastreiMotoristasHom\CadastreiMotoristasHom.exe"
    $exeAfastamentos = Join-Path $BaseDir "apps\hom\CadastreiAfastamentosHom\CadastreiAfastamentosHom.exe"
}

if (-not (Test-Path $exeMotoristas)) {
    throw "Executavel de motoristas nao encontrado: $exeMotoristas"
}
if (-not (Test-Path $exeAfastamentos)) {
    throw "Executavel de afastamentos nao encontrado: $exeAfastamentos"
}

if ([string]::IsNullOrWhiteSpace($ServicoMotoristas)) {
    if ($Ambiente -eq "Producao") { $ServicoMotoristas = "CadastreiMotoristasProd" }
    else { $ServicoMotoristas = "CadastreiMotoristasHom" }
}
if ([string]::IsNullOrWhiteSpace($ServicoAfastamentos)) {
    if ($Ambiente -eq "Producao") { $ServicoAfastamentos = "CadastreiAfastamentosProd" }
    else { $ServicoAfastamentos = "CadastreiAfastamentosHom" }
}

if ($Ambiente -eq "Producao") { $origemDb = "Vetorh_Prod" } else { $origemDb = "Vetorh_Hom" }

if ($Ambiente -eq "Producao") {
    $logMotoristas = "logs\motoristas_prod.log"
    $logAfastamentos = "logs\afastamentos_prod.log"
} else {
    $logMotoristas = "logs\motoristas_hom.log"
    $logAfastamentos = "logs\afastamentos_hom.log"
}

$argsMotoristas = "--origem-db $origemDb --destino-db Cadastrei --intervalo $IntervaloSegundos --batch-size $BatchSize --log-file $logMotoristas"
$argsAfastamentos = "--origem-db $origemDb --destino-db Cadastrei --intervalo $IntervaloSegundos --batch-size $BatchSize --log-file $logAfastamentos"
if (-not [string]::IsNullOrWhiteSpace($DataInicioAfastamentos)) {
    $argsAfastamentos = "$argsAfastamentos --data-inicio $DataInicioAfastamentos"
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

Start-ServiceChecked -Name $ServicoMotoristas -StdoutLog $stdoutMotoristas -StderrLog $stderrMotoristas
Start-ServiceChecked -Name $ServicoAfastamentos -StdoutLog $stdoutAfastamentos -StderrLog $stderrAfastamentos

Write-Host "Servicos configurados e iniciados com sucesso."
Write-Host "Motoristas:   $ServicoMotoristas"
Write-Host "Afastamentos: $ServicoAfastamentos"
Write-Host "Ambiente:     $Ambiente (origem=$origemDb)"
Write-Host "Base:         $BaseDir"
