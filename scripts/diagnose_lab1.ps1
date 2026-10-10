# Usage from C:\...\GitHub\lab02:
#   powershell -ExecutionPolicy Bypass -File .\scripts\diagnose_lab1.ps1 -Namespace bd-g06 -Lab1Path ..\lab
# Read-only diagnostics: no deletion, no changes to cluster and no secret-value export.
param(
    [string]$Namespace = 'bd-g06',
    [string]$Lab1Path = '..\lab'
)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$evidenceDir = Join-Path $repo 'work\evidence'
New-Item -ItemType Directory -Force -Path $evidenceDir | Out-Null
$report = Join-Path $evidenceDir 'k8s-diagnostic.txt'
$lines = New-Object System.Collections.Generic.List[string]
function Out-Line([string]$Message) { Write-Host $Message; $script:lines.Add($Message) }
function Run-Kubectl([string]$Label, [string[]]$KubectlArguments) {
    Out-Line "`n=== $Label ==="
    $output = & kubectl @KubectlArguments 2>&1
    $exit = $LASTEXITCODE
    foreach ($line in $output) { Out-Line ([string]$line) }
    if ($exit -ne 0) { Out-Line "kubectl exit code: $exit" }
}
try { & kubectl version --client | Out-Null } catch {
    throw 'kubectl is not installed or not on PATH.'
}
Out-Line "LAB2 T1 Kubernetes diagnostic"
Out-Line "Timestamp local: $((Get-Date).ToString('o'))"
Out-Line "Namespace: $Namespace"
Run-Kubectl 'Current context' @('config', 'current-context')
Run-Kubectl 'Nodes' @('get','nodes','-o','wide')
Run-Kubectl 'Namespace resources' @('get','pods,svc,deploy,sts,pvc,resourcequota,limitrange,networkpolicy','-n',$Namespace)
Run-Kubectl 'Secret names only; never values' @('get','secrets','-n',$Namespace,'-o','name')
Run-Kubectl 'Curate pod' @('get','pod','curate','-n',$Namespace,'-o','wide')
Run-Kubectl 'Curate pod description/events' @('describe','pod','curate','-n',$Namespace)
Run-Kubectl 'Namespace events' @('get','events','-n',$Namespace,'--sort-by=.lastTimestamp')
Run-Kubectl 'All object storage services (names only)' @('get','svc','-A')
Out-Line "`n=== Suggested diagnosis ==="
$podJsonRaw = & kubectl get pod curate -n $Namespace -o json 2>$null
if ($LASTEXITCODE -eq 0) {
    $pod = $podJsonRaw | ConvertFrom-Json
    Out-Line "Pod phase: $($pod.status.phase)"
    foreach ($container in $pod.spec.containers) { Out-Line "Container '$($container.name)' image: $($container.image)" }
    foreach ($container in @($pod.status.containerStatuses)) {
        if ($null -ne $container -and $null -ne $container.state.waiting) {
            Out-Line "Waiting reason: $($container.state.waiting.reason)"
            Out-Line "Waiting message: $($container.state.waiting.message)"
        }
    }
    if (($pod.spec.containers | ForEach-Object { $_.image }) -match 'REPLACE_WITH') {
        Out-Line 'ACTION: Replace placeholder with actual approved image @sha256 digest; do NOT guess.'
    }
} else { Out-Line 'Curate pod not found.' }
& kubectl get secret s3-ingestor -n $Namespace -o name *> $null
if ($LASTEXITCODE -ne 0) { Out-Line 'BLOCKER: Secret s3-ingestor missing; restore from Lab 1 authorized setup (do not commit credentials).' }
& kubectl get svc objects -n $Namespace -o name *> $null
if ($LASTEXITCODE -ne 0) { Out-Line 'BLOCKER: Service objects missing; restore Lab 1 object storage before S3 ingestion.' }
$labPath = Join-Path $repo $Lab1Path
Out-Line "`n=== Lab 1 manifest candidates (filenames only) ==="
if (Test-Path $labPath) {
    $matches = Get-ChildItem -Path $labPath -Recurse -File -Include *.yaml,*.yml -ErrorAction SilentlyContinue |
        Select-String -Pattern 'name:\s*objects|s3-ingestor|research-raw|kind:\s*StatefulSet' |
        Select-Object -ExpandProperty Path -Unique
    if ($matches) { foreach ($file in $matches) { Out-Line $file } }
    else { Out-Line 'No matching manifest files found in the specified Lab 1 repo.' }
} else { Out-Line "Lab 1 path not found: $labPath" }
Out-Line "`nNo cluster resources were changed."
$lines | Set-Content -Path $report -Encoding UTF8
Write-Host "`nDiagnostic saved to: $report"
