param(
    [string]$DestinoRaiz = "C:\Cadastrei",
    [ValidateSet("Todos", "Producao", "Homologacao")]
    [string]$Ambiente = "Todos",
    [switch]$IncluirInterface
)

$ErrorActionPreference = "Stop"

$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$python = Join-Path $repo ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    throw "Python do virtualenv nao encontrado em $python"
}

$appsProdPath = Join-Path $DestinoRaiz "apps\prod"
$appsHomPath = Join-Path $DestinoRaiz "apps\hom"
$appsUiPath = Join-Path $DestinoRaiz "apps\ui"
$deployPath = Join-Path $DestinoRaiz "deploy"
$workPath = Join-Path $DestinoRaiz "build\work"
$specPath = Join-Path $DestinoRaiz "build\spec"
$logsPath = Join-Path $DestinoRaiz "logs"

New-Item -ItemType Directory -Path $appsProdPath -Force | Out-Null
New-Item -ItemType Directory -Path $appsHomPath -Force | Out-Null
New-Item -ItemType Directory -Path $appsUiPath -Force | Out-Null
New-Item -ItemType Directory -Path $deployPath -Force | Out-Null
New-Item -ItemType Directory -Path $workPath -Force | Out-Null
New-Item -ItemType Directory -Path $specPath -Force | Out-Null
New-Item -ItemType Directory -Path $logsPath -Force | Out-Null

Write-Host "Verificando PyInstaller..."
& $python -m pip show pyinstaller | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Instalando PyInstaller..."
    & $python -m pip install pyinstaller
}

function Invoke-BuildTarget {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$ScriptRelativePath,
        [Parameter(Mandatory = $true)][string]$DistPath,
        [switch]$Windowed
    )

    $scriptPath = Join-Path $repo $ScriptRelativePath
    if (-not (Test-Path $scriptPath)) {
        throw "Script de entrada nao encontrado: $scriptPath"
    }

    $cmd = @(
        "-m", "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onedir",
        "--name", $Name,
        "--distpath", $DistPath,
        "--workpath", $workPath,
        "--specpath", $specPath,
        "--paths", $repo
    )

    if ($Windowed) {
        $cmd += "--windowed"
    }
    $cmd += $scriptPath

    Write-Host "Gerando executavel: $Name"
    & $python @cmd
    if ($LASTEXITCODE -ne 0) {
        throw "Falha no build do executavel $Name"
    }
}

$buildProd = $Ambiente -in @("Todos", "Producao")
$buildHom = $Ambiente -in @("Todos", "Homologacao")

if ($buildProd) {
    Invoke-BuildTarget -Name "CadastreiMotoristasProd" -ScriptRelativePath "scripts\servico_motoristas_prod.py" -DistPath $appsProdPath
    Invoke-BuildTarget -Name "CadastreiAfastamentosProd" -ScriptRelativePath "scripts\servico_afastamentos_prod.py" -DistPath $appsProdPath
}

if ($buildHom) {
    Invoke-BuildTarget -Name "CadastreiMotoristasHom" -ScriptRelativePath "scripts\servico_motoristas_hom.py" -DistPath $appsHomPath
    Invoke-BuildTarget -Name "CadastreiAfastamentosHom" -ScriptRelativePath "scripts\servico_afastamentos_hom.py" -DistPath $appsHomPath
}

if ($IncluirInterface) {
    Invoke-BuildTarget -Name "CadastreiInterface" -ScriptRelativePath "main.py" -DistPath $appsUiPath -Windowed
}

$envOrigem = Join-Path $repo ".env"
$envDestino = Join-Path $DestinoRaiz ".env"
if (Test-Path $envOrigem) {
    Copy-Item $envOrigem $envDestino -Force
    Write-Host "Arquivo .env copiado para $envDestino"
} else {
Write-Host "Arquivo .env nao encontrado no repositorio. Copie manualmente para $envDestino"
}

$envExampleOrigem = Join-Path $repo ".env.example"
$envExampleDestino = Join-Path $DestinoRaiz ".env.example"
if (Test-Path $envExampleOrigem) {
    Copy-Item $envExampleOrigem $envExampleDestino -Force
}

$installScriptOrigem = Join-Path $repo "scripts\instalar_servicos_nssm.ps1"
$installScriptDestino = Join-Path $deployPath "instalar_servicos_nssm.ps1"
if (Test-Path $installScriptOrigem) {
    Copy-Item $installScriptOrigem $installScriptDestino -Force
}

Write-Host ""
Write-Host "Build concluido."
if ($buildProd) {
    Write-Host "Executaveis de producao:   $appsProdPath"
}
if ($buildHom) {
    Write-Host "Executaveis de homologacao: $appsHomPath"
}
if ($IncluirInterface) {
    Write-Host "Executavel da interface:    $appsUiPath"
}
Write-Host "Scripts de deploy:          $deployPath"
Write-Host "Logs devem ser gravados em: $logsPath"
