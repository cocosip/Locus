param(
    [switch]$SkipRun
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$resultsDir = Join-Path $repoRoot "BenchmarkDotNet.Artifacts\results"
$largeCsv = Join-Path $resultsDir "Locus.Benchmarks.FileWatcherLargeDirectoryBenchmarks-report.csv"
$dispatchCsv = Join-Path $resultsDir "Locus.Benchmarks.FileWatcherConfigurationDispatchBenchmarks-report.csv"
$multiCsv = Join-Path $resultsDir "Locus.Benchmarks.FileWatcherMultiTenantStatusBenchmarks-report.csv"
$pruneCsv = Join-Path $resultsDir "Locus.Benchmarks.FileWatcherPrunePressureBenchmarks-report.csv"
$panelPath = Join-Path $resultsDir "Locus.Benchmarks.Phase14-Benchmark-Panel.md"

function Parse-MeanMilliseconds([string]$value) {
    if ([string]::IsNullOrWhiteSpace($value)) {
        return [double]::NaN
    }

    $trimmed = $value.Trim().Replace([char]0x00B5, "u").Replace([char]0x03BC, "u").Replace([char]0x00A0, " ")
    if ($trimmed -notmatch '^([0-9][0-9,]*(?:\.[0-9]+)?)\s*(ns|us|ms|s)$') {
        return [double]::NaN
    }

    $numeric = $matches[1].Replace(",", "")
    $raw = [double]::Parse($numeric, [System.Globalization.CultureInfo]::InvariantCulture)
    $unit = $matches[2]
    switch ($unit) {
        "s"  { return $raw * 1000.0 }
        "ms" { return $raw }
        "us" { return $raw / 1000.0 }
        "ns" { return $raw / 1000000.0 }
        default { return [double]::NaN }
    }
}

function Add-Lines($target, $lines) {
    foreach ($line in $lines) {
        $target.Add([string]$line)
    }
}

function Build-MatrixMarkdown($rows, $rowKey, $colKey) {
    $rowValues = $rows | ForEach-Object { [int]($_.$rowKey) } | Sort-Object -Unique
    $colValues = $rows | ForEach-Object { [int]($_.$colKey) } | Sort-Object -Unique

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("| $rowKey \\ $colKey | " + (($colValues | ForEach-Object { "$_" }) -join " | ") + " |")
    $lines.Add("|---" + ("|---" * $colValues.Count) + "|")

    foreach ($rowVal in $rowValues) {
        $cells = foreach ($colVal in $colValues) {
            $match = $rows | Where-Object { [int]($_.$rowKey) -eq $rowVal -and [int]($_.$colKey) -eq $colVal } | Select-Object -First 1
            if ($null -eq $match) { "N/A" } else { $match.Mean }
        }
        $lines.Add("| $rowVal | " + ($cells -join " | ") + " |")
    }

    return $lines
}

function Build-DetailTableMarkdown($rows, $columns) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("| " + ($columns -join " | ") + " |")
    $lines.Add("|" + (($columns | ForEach-Object { "---" }) -join "|") + "|")
    foreach ($row in $rows) {
        $values = foreach ($col in $columns) { $row.$col }
        $lines.Add("| " + ($values -join " | ") + " |")
    }
    return $lines
}

function With-Ratio($rows, $baselineMean) {
    return $rows | ForEach-Object {
        $meanMs = Parse-MeanMilliseconds $_.Mean
        $ratio = if ($baselineMean -gt 0 -and -not [double]::IsNaN($meanMs)) {
            "{0:F2}x" -f ($meanMs / $baselineMean)
        } else {
            "N/A"
        }

        $obj = [ordered]@{}
        foreach ($prop in $_.PSObject.Properties) {
            $obj[$prop.Name] = $prop.Value
        }
        $obj["RatioVsBaseline"] = $ratio
        [PSCustomObject]$obj
    }
}

Push-Location $repoRoot
try {
    if (-not $SkipRun) {
        dotnet run -c Release --project tests/Locus.Benchmarks --filter "Locus.Benchmarks.FileWatcherLargeDirectoryBenchmarks*"
        dotnet run -c Release --project tests/Locus.Benchmarks --filter "Locus.Benchmarks.FileWatcherConfigurationDispatchBenchmarks*"
        dotnet run -c Release --project tests/Locus.Benchmarks --filter "Locus.Benchmarks.FileWatcherMultiTenantStatusBenchmarks*"
        dotnet run -c Release --project tests/Locus.Benchmarks --filter "Locus.Benchmarks.FileWatcherPrunePressureBenchmarks*"
    }

    foreach ($csv in @($largeCsv, $dispatchCsv, $multiCsv, $pruneCsv)) {
        if (-not (Test-Path $csv)) { throw "Missing benchmark result: $csv" }
    }

    $largeRows = Import-Csv $largeCsv | Sort-Object { [int]$_.FileCount }, { [int]$_.MaxConcurrentImports }
    $dispatchRows = Import-Csv $dispatchCsv | Sort-Object Method
    $multiRows = Import-Csv $multiCsv | Sort-Object { [int]$_.TenantDirectoryCount }, { [int]$_.DisabledRatioPercent }
    $pruneRows = Import-Csv $pruneCsv | Sort-Object { [int]$_.ExistingImportedEntries }, { [int]$_.NewFiles }

    $largeBaseline = $largeRows | Select-Object -First 1
    $multiBaseline = $multiRows | Select-Object -First 1
    $pruneBaseline = $pruneRows | Select-Object -First 1
    $dispatchBaseline = $dispatchRows | Where-Object { $_.Method -like "*watcher id*" } | Select-Object -First 1
    if ($null -eq $dispatchBaseline) { $dispatchBaseline = $dispatchRows | Select-Object -First 1 }

    $largeWithRatio = With-Ratio $largeRows (Parse-MeanMilliseconds $largeBaseline.Mean)
    $dispatchWithRatio = With-Ratio $dispatchRows (Parse-MeanMilliseconds $dispatchBaseline.Mean)
    $multiWithRatio = With-Ratio $multiRows (Parse-MeanMilliseconds $multiBaseline.Mean)
    $pruneWithRatio = With-Ratio $pruneRows (Parse-MeanMilliseconds $pruneBaseline.Mean)

    $content = New-Object System.Collections.Generic.List[string]
    $content.Add("# Phase 1-4 Benchmark Panel")
    $content.Add("")
    $content.Add("Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')")
    $content.Add("")

    $content.Add("## Point 1 - Large Directory Streamed Scan")
    $content.Add("")
    $content.Add("Baseline: FileCount=$($largeBaseline.FileCount), MaxConcurrentImports=$($largeBaseline.MaxConcurrentImports), Mean=$($largeBaseline.Mean)")
    $content.Add("")
    Add-Lines $content (Build-MatrixMarkdown $largeRows "FileCount" "MaxConcurrentImports")
    $content.Add("")
    Add-Lines $content (Build-DetailTableMarkdown $largeWithRatio @("FileCount","MaxConcurrentImports","Mean","RatioVsBaseline","Error","StdDev","Allocated"))
    $content.Add("")

    $content.Add("## Point 2 - Config Dispatch")
    $content.Add("")
    $content.Add("Baseline: Method=$($dispatchBaseline.Method), Mean=$($dispatchBaseline.Mean)")
    $content.Add("")
    Add-Lines $content (Build-DetailTableMarkdown $dispatchWithRatio @("Method","Mean","RatioVsBaseline","Error","StdDev","Allocated"))
    $content.Add("")

    $content.Add("## Point 3 - Multi-Tenant Status Reuse")
    $content.Add("")
    $content.Add("Baseline: TenantDirectoryCount=$($multiBaseline.TenantDirectoryCount), DisabledRatioPercent=$($multiBaseline.DisabledRatioPercent), Mean=$($multiBaseline.Mean)")
    $content.Add("")
    Add-Lines $content (Build-MatrixMarkdown $multiRows "TenantDirectoryCount" "DisabledRatioPercent")
    $content.Add("")
    Add-Lines $content (Build-DetailTableMarkdown $multiWithRatio @("TenantDirectoryCount","DisabledRatioPercent","Mean","RatioVsBaseline","Error","StdDev","Allocated"))
    $content.Add("")

    $content.Add("## Point 4 - Prune Off Hot Path (Batch-Tail Prune)")
    $content.Add("")
    $content.Add("Baseline: ExistingImportedEntries=$($pruneBaseline.ExistingImportedEntries), NewFiles=$($pruneBaseline.NewFiles), Mean=$($pruneBaseline.Mean)")
    $content.Add("")
    Add-Lines $content (Build-MatrixMarkdown $pruneRows "ExistingImportedEntries" "NewFiles")
    $content.Add("")
    Add-Lines $content (Build-DetailTableMarkdown $pruneWithRatio @("ExistingImportedEntries","NewFiles","Mean","RatioVsBaseline","Error","StdDev","Allocated"))
    $content.Add("")

    $content.Add("## Source Files")
    $content.Add("")
    $content.Add("- $largeCsv")
    $content.Add("- $dispatchCsv")
    $content.Add("- $multiCsv")
    $content.Add("- $pruneCsv")

    Set-Content -Path $panelPath -Value $content -Encoding UTF8
    Write-Host "Generated panel: $panelPath"
}
finally {
    Pop-Location
}
