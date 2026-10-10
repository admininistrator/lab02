# Executes official S3-mode T1 after the original Lab 1 Service + Secret and curate pod are restored.
# Usage: powershell -ExecutionPolicy Bypass -File .\scripts\run_s3_t1.ps1 -Namespace bd-g06
param([string]$Namespace = 'bd-g06')
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$manifest = Join-Path $repo 'fixture\trusted_manifest.json'
$stage = Join-Path $repo 'scripts\stage_inputs.py'
$access = Join-Path $repo 'scripts\check_s3_access.py'
$out = Join-Path $repo 'work\s3'
New-Item -ItemType Directory -Path $out -Force | Out-Null
foreach ($path in @($manifest, $stage, $access)) {
    if (-not (Test-Path $path)) { throw "Missing local file: $path" }
}
function Require-Kube([string[]]$Arguments, [string]$ErrorMessage) {
    & kubectl @Arguments
    if ($LASTEXITCODE -ne 0) { throw $ErrorMessage }
}
Write-Host 'Checking Kubernetes prerequisites...'
Require-Kube @('get','svc','objects','-n',$Namespace,'-o','name') 'Lab 1 objects Service missing.'
Require-Kube @('get','secret','s3-ingestor','-n',$Namespace,'-o','name') 'Lab 1 s3-ingestor Secret missing.'
$podRaw = & kubectl get pod curate -n $Namespace -o json
if ($LASTEXITCODE -ne 0) { throw 'Curate Pod missing.' }
$pod = $podRaw | ConvertFrom-Json
if ($pod.status.phase -ne 'Running') {
    throw "Curate pod phase=$($pod.status.phase). Run diagnose_lab1.ps1 before retrying."
}
$client = @($pod.status.containerStatuses | Where-Object { $_.name -eq 'client' }) | Select-Object -First 1
if ($null -eq $client -or -not $client.ready) { throw 'Container client is not Ready.' }
Write-Host 'Checking whether tar is present for kubectl cp...'
Require-Kube @('-n',$Namespace,'exec','curate','-c','client','--','sh','-c','command -v tar') 'Container needs tar for kubectl cp; use a prepared image with tar or arrange a safe copy alternative.'
Write-Host 'Copying code and independently supplied manifest into curate Pod...'
Require-Kube @('-n',$Namespace,'cp',$stage,'curate:/work/stage_inputs.py','-c','client') 'Copy stage_inputs.py failed.'
Require-Kube @('-n',$Namespace,'cp',$access,'curate:/work/check_s3_access.py','-c','client') 'Copy check_s3_access.py failed.'
Require-Kube @('-n',$Namespace,'cp',$manifest,'curate:/work/trusted_manifest.json','-c','client') 'Copy trusted_manifest.json failed.'
Write-Host 'Running T1 input verification through the S3 curator identity...'
Require-Kube @('-n',$Namespace,'exec','curate','-c','client','--','python','stage_inputs.py','s3','trusted_manifest.json','--output-dir','.') 'S3 ingestion failed; do not label I01/I02 as S3 PASS.'
Write-Host 'Running real raw GET / forbidden release PUT probe...'
Require-Kube @('-n',$Namespace,'exec','curate','-c','client','--','python','check_s3_access.py') 'I04 failed or was blocked. Inspect denied result; do not label I04 PASS.'
Write-Host 'Copying evidence to ignored work/s3 folder...'
foreach ($name in @('input_inventory.json','envelopes.csv','access-test.json')) {
    Require-Kube @('-n',$Namespace,'cp',"curate:/work/$name",(Join-Path $out $name),'-c','client') "Could not copy $name."
}
Require-Kube @('-n',$Namespace,'cp','curate:/work/evidence/I04.json',(Join-Path $out 'I04.json'),'-c','client') 'Could not copy I04.json.'
$record = [ordered]@{
    utc_time=(Get-Date).ToUniversalTime().ToString('o'); namespace=$Namespace;
    pod='curate'; workload_identity='s3-ingestor'; store_id='bd-g06-objects';
    image=$pod.spec.containers[0].image; image_id=$client.imageID;
    trusted_manifest_sha256=(Get-FileHash -Path $manifest -Algorithm SHA256).Hash.ToLowerInvariant();
    contract_sha256=(Get-FileHash -Path (Join-Path $repo 'contract.json') -Algorithm SHA256).Hash.ToLowerInvariant();
    source_mode='s3'; i04_result='see I04.json'; independent_review='PENDING unless separately signed off'
}
$record | ConvertTo-Json -Depth 5 | Set-Content -Path (Join-Path $out 's3-run-context.json') -Encoding UTF8
Write-Host 'S3 stage and I04 succeeded. Evidence in work/s3; DO NOT commit restricted envelopes.csv.'
