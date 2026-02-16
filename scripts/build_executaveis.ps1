param(
    [string]$DestinoRaiz = "C:\Cadastrei",
    [ValidateSet("Todos", "Producao", "Homologacao")]
    [string]$Ambiente = "Todos",
    [switch]$IncluirInterface,
    [switch]$NaoPararProcessos
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

function Ensure-PythonModule {
    param(
        [Parameter(Mandatory = $true)][string]$ModuleName
    )
    & $python -m pip show $ModuleName | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Instalando dependencia ausente: $ModuleName"
        & $python -m pip install $ModuleName
        if ($LASTEXITCODE -ne 0) {
            throw "Falha ao instalar modulo $ModuleName no virtualenv."
        }
    }
}

# Dependencias minimas para os executaveis de servico.
Ensure-PythonModule -ModuleName "sqlalchemy"
Ensure-PythonModule -ModuleName "pyodbc"
Ensure-PythonModule -ModuleName "pydantic"
Ensure-PythonModule -ModuleName "python-dotenv"
Ensure-PythonModule -ModuleName "httpx"

function Stop-TargetProcessIfRunning {
    param(
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($NaoPararProcessos) {
        return
    }

    $procs = Get-Process -Name $Name -ErrorAction SilentlyContinue
    if ($procs) {
        Write-Host "Encerrando processo em execucao: $Name"
        $procs | Stop-Process -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
    }
}

function Remove-TargetDirWithRetry {
    param(
        [Parameter(Mandatory = $true)][string]$DistPath,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $targetDir = Join-Path $DistPath $Name
    if (-not (Test-Path $targetDir)) {
        return
    }

    for ($tentativa = 1; $tentativa -le 8; $tentativa++) {
        try {
            Remove-Item $targetDir -Recurse -Force -ErrorAction Stop
            return
        } catch {
            Stop-TargetProcessIfRunning -Name $Name
            Start-Sleep -Milliseconds 800
        }
    }

    throw "Nao foi possivel limpar a pasta de build '$targetDir'. Feche o executavel em uso e tente novamente."
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

    Stop-TargetProcessIfRunning -Name $Name
    Remove-TargetDirWithRetry -DistPath $DistPath -Name $Name

    $cmd = @(
        "-m", "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onedir",
        "--name", $Name,
        "--distpath", $DistPath,
        "--workpath", $workPath,
        "--specpath", $specPath,
        "--paths", $repo,
        "--hidden-import", "pyodbc",
        "--hidden-import", "sqlalchemy.dialects.mssql.pyodbc",
        "--hidden-import", "httpx",
        "--hidden-import", "httpcore",
        "--hidden-import", "anyio",
        "--hidden-import", "sniffio",
        "--hidden-import", "certifi",
        "--hidden-import", "idna",
        "--collect-binaries", "pyodbc"
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
    Invoke-BuildTarget -Name "CadastreiApiMotoristasProd" -ScriptRelativePath "scripts\servico_api_motoristas.py" -DistPath $appsProdPath
    Invoke-BuildTarget -Name "CadastreiApiAfastamentosProd" -ScriptRelativePath "scripts\servico_api_afastamentos.py" -DistPath $appsProdPath
}

if ($buildHom) {
    Invoke-BuildTarget -Name "CadastreiMotoristasHom" -ScriptRelativePath "scripts\servico_motoristas_hom.py" -DistPath $appsHomPath
    Invoke-BuildTarget -Name "CadastreiAfastamentosHom" -ScriptRelativePath "scripts\servico_afastamentos_hom.py" -DistPath $appsHomPath
    Invoke-BuildTarget -Name "CadastreiApiMotoristasHom" -ScriptRelativePath "scripts\servico_api_motoristas.py" -DistPath $appsHomPath
    Invoke-BuildTarget -Name "CadastreiApiAfastamentosHom" -ScriptRelativePath "scripts\servico_api_afastamentos.py" -DistPath $appsHomPath
}

if ($IncluirInterface) {
    Invoke-BuildTarget -Name "CadastreiInterface" -ScriptRelativePath "main.py" -DistPath $appsUiPath -Windowed
}

$envOrigem = Join-Path $repo ".env"
$envDestino = Join-Path $DestinoRaiz ".env"
if (Test-Path $envOrigem) {
    Copy-Item $envOrigem $envDestino -Force
    Write-Host "Arquivo .env copiado para $envDestino"

    $targetsEnv = @()
    if ($buildProd) {
        $targetsEnv += (Join-Path $appsProdPath "CadastreiMotoristasProd\.env")
        $targetsEnv += (Join-Path $appsProdPath "CadastreiAfastamentosProd\.env")
        $targetsEnv += (Join-Path $appsProdPath "CadastreiApiMotoristasProd\.env")
        $targetsEnv += (Join-Path $appsProdPath "CadastreiApiAfastamentosProd\.env")
    }
    if ($buildHom) {
        $targetsEnv += (Join-Path $appsHomPath "CadastreiMotoristasHom\.env")
        $targetsEnv += (Join-Path $appsHomPath "CadastreiAfastamentosHom\.env")
        $targetsEnv += (Join-Path $appsHomPath "CadastreiApiMotoristasHom\.env")
        $targetsEnv += (Join-Path $appsHomPath "CadastreiApiAfastamentosHom\.env")
    }
    if ($IncluirInterface) {
        $targetsEnv += (Join-Path $appsUiPath "CadastreiInterface\.env")
    }

    foreach ($target in $targetsEnv) {
        $targetDir = Split-Path -Parent $target
        if (Test-Path $targetDir) {
            Copy-Item $envOrigem $target -Force
        }
    }
} else {
    Write-Host "Arquivo .env nao encontrado no repositorio. Copie manualmente para $envDestino"
}

$envExampleOrigem = Join-Path $repo ".env.example"
$envExampleDestino = Join-Path $DestinoRaiz ".env.example"
if (Test-Path $envExampleOrigem) {
    Copy-Item $envExampleOrigem $envExampleDestino -Force

    $targetsEnvExample = @()
    if ($buildProd) {
        $targetsEnvExample += (Join-Path $appsProdPath "CadastreiMotoristasProd\.env.example")
        $targetsEnvExample += (Join-Path $appsProdPath "CadastreiAfastamentosProd\.env.example")
        $targetsEnvExample += (Join-Path $appsProdPath "CadastreiApiMotoristasProd\.env.example")
        $targetsEnvExample += (Join-Path $appsProdPath "CadastreiApiAfastamentosProd\.env.example")
    }
    if ($buildHom) {
        $targetsEnvExample += (Join-Path $appsHomPath "CadastreiMotoristasHom\.env.example")
        $targetsEnvExample += (Join-Path $appsHomPath "CadastreiAfastamentosHom\.env.example")
        $targetsEnvExample += (Join-Path $appsHomPath "CadastreiApiMotoristasHom\.env.example")
        $targetsEnvExample += (Join-Path $appsHomPath "CadastreiApiAfastamentosHom\.env.example")
    }
    if ($IncluirInterface) {
        $targetsEnvExample += (Join-Path $appsUiPath "CadastreiInterface\.env.example")
    }

    foreach ($target in $targetsEnvExample) {
        $targetDir = Split-Path -Parent $target
        if (Test-Path $targetDir) {
            Copy-Item $envExampleOrigem $target -Force
        }
    }
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
