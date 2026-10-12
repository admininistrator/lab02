param(
    [string]$BeforePath = (Join-Path $PSScriptRoot 'quality_before.json'),
    [string]$AfterPath = (Join-Path $PSScriptRoot 'quality_after.json'),
    [string]$OutputPath = (Join-Path $PSScriptRoot 'quality_before_after.svg')
)

$before = Get-Content -Raw -LiteralPath $BeforePath | ConvertFrom-Json
$after = Get-Content -Raw -LiteralPath $AfterPath | ConvertFrom-Json
$labels = [ordered]@{
    Q_REQUIRED = 'Required-field completeness'
    Q_KEY_UNIQUENESS = 'Business-key uniqueness'
    Q_VALUE = 'Value validity'
    Q_SENSOR = 'Registry consistency'
    Q_CHRONOLOGY = 'Temporal consistency'
    Q_TIMELINESS = 'Timeliness (eligible rows)'
    Q_QA_COVERAGE = 'Reference coverage'
    Q_QA_AGREEMENT = 'Reference agreement'
}
$beforeById = @{}
$afterById = @{}
foreach ($metric in $before.metrics) { $beforeById[$metric.rule_id] = $metric }
foreach ($metric in $after.metrics) { $afterById[$metric.rule_id] = $metric }

$left = 250; $barWidth = 300; $top = 74; $rowHeight = 42
$height = $top + ($labels.Count * $rowHeight) + 34
$lines = [System.Collections.Generic.List[string]]::new()
$lines.Add("<svg xmlns='http://www.w3.org/2000/svg' width='1040' height='$height' viewBox='0 0 1040 $height'>")
$lines.Add('<rect width="100%" height="100%" fill="#ffffff"/>')
$lines.Add('<text x="24" y="30" font-family="Arial" font-size="20" font-weight="bold">Lab 2 quality: before winners vs curated candidate</text>')
$lines.Add('<text x="24" y="52" font-family="Arial" font-size="12" fill="#444">Before = W winners before validity filtering; after = curated rows (reference metrics evaluated after curation)</text>')
$lines.Add('<text x="250" y="69" font-family="Arial" font-size="12" fill="#333">0%</text>')
$lines.Add('<text x="530" y="69" font-family="Arial" font-size="12" fill="#333">100%</text>')
$i = 0
foreach ($rule in $labels.Keys) {
    $y = $top + ($i * $rowHeight)
    $lines.Add("<text x='24' y='$($y + 17)' font-family='Arial' font-size='13'>$($labels[$rule])</text>")
    $lines.Add("<rect x='$left' y='$($y + 3)' width='$barWidth' height='14' rx='3' fill='#eeeeee'/>")
    if ($beforeById.ContainsKey($rule)) {
        $m = $beforeById[$rule]; $pct = [double]$m.value; $w = [Math]::Round($barWidth * $pct, 1)
        $lines.Add("<rect x='$left' y='$($y + 3)' width='$w' height='14' rx='3' fill='#6b8afd'/>")
        $lines.Add("<text x='570' y='$($y + 15)' font-family='Arial' font-size='12'>before $($m.numerator)/$($m.denominator) ($([Math]::Round($pct * 100, 2))%)</text>")
    } else {
        $lines.Add("<text x='570' y='$($y + 15)' font-family='Arial' font-size='12' fill='#777'>before: not measured for this rule</text>")
    }
    $lines.Add("<rect x='$left' y='$($y + 21)' width='$barWidth' height='14' rx='3' fill='#eeeeee'/>")
    if ($afterById.ContainsKey($rule)) {
        $m = $afterById[$rule]; $pct = [double]$m.value; $w = [Math]::Round($barWidth * $pct, 1)
        $lines.Add("<rect x='$left' y='$($y + 21)' width='$w' height='14' rx='3' fill='#22a06b'/>")
        $lines.Add("<text x='570' y='$($y + 33)' font-family='Arial' font-size='12'>after $($m.numerator)/$($m.denominator) ($([Math]::Round($pct * 100, 2))%)</text>")
    } else {
        $lines.Add("<text x='570' y='$($y + 33)' font-family='Arial' font-size='12' fill='#777'>after: not measured</text>")
    }
    $i++
}
$lines.Add("<text x='24' y='$($height - 10)' font-family='Arial' font-size='11' fill='#444'>Blue = before; green = after. Denominators are shown per metric; timeliness uses chronologically eligible rows.</text>")
$lines.Add('</svg>')
Set-Content -LiteralPath $OutputPath -Value ($lines -join "`n") -Encoding utf8
Write-Output "Wrote $OutputPath from metric records in $BeforePath and $AfterPath"
