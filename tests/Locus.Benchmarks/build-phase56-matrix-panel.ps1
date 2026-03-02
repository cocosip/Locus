param(
    [switch]$SkipRun
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$resultsDir = Join-Path $repoRoot "BenchmarkDotNet.Artifacts\results"
$phase5Csv = Join-Path $resultsDir "Locus.Benchmarks.DirectoryQuotaDirtyFlushBenchmarks-report.csv"
$phase6Csv = Join-Path $resultsDir "Locus.Benchmarks.KnownDirectoryTrimBenchmarks-report.csv"
$panelPath = Join-Path $resultsDir "Locus.Benchmarks.Phase56-Matrix-Panel.md"

function Format-MethodName([string]$name) {
    return $name.Trim("'")
}

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

function Build-Phase5MatrixMarkdown($rows) {
    $totalValues = $rows | ForEach-Object { [int]$_.TotalDirectories } | Sort-Object -Unique
    $dirtyValues = $rows | ForEach-Object { [int]$_.DirtyDirectories } | Sort-Object -Unique

    $lines = New-Object System.Collections.Generic.List[string]
    $header = "| TotalDirectories \ DirtyDirectories | " + (($dirtyValues | ForEach-Object { "$_" }) -join " | ") + " |"
    $separator = "|---" + ("|---" * $dirtyValues.Count) + "|"
    $lines.Add($header)
    $lines.Add($separator)

    foreach ($total in $totalValues) {
        $cells = foreach ($dirty in $dirtyValues) {
            $row = $rows | Where-Object { [int]$_.TotalDirectories -eq $total -and [int]$_.DirtyDirectories -eq $dirty } | Select-Object -First 1
            if ($null -eq $row) { "N/A" } else { $row.Mean }
        }
        $lines.Add("| $total | " + ($cells -join " | ") + " |")
    }

    return $lines
}

function Build-Phase6MatrixMarkdown($rows) {
    $cacheValues = $rows | ForEach-Object { [int]$_.CacheMaxEntries } | Sort-Object -Unique
    $addValues = $rows | ForEach-Object { [int]$_.DirectoryAddsPerOperation } | Sort-Object -Unique

    $lines = New-Object System.Collections.Generic.List[string]
    $header = "| CacheMaxEntries \ DirectoryAddsPerOperation | " + (($addValues | ForEach-Object { "$_" }) -join " | ") + " |"
    $separator = "|---" + ("|---" * $addValues.Count) + "|"
    $lines.Add($header)
    $lines.Add($separator)

    foreach ($cache in $cacheValues) {
        $cells = foreach ($adds in $addValues) {
            $row = $rows | Where-Object { [int]$_.CacheMaxEntries -eq $cache -and [int]$_.DirectoryAddsPerOperation -eq $adds } | Select-Object -First 1
            if ($null -eq $row) { "N/A" } else { $row.Mean }
        }
        $lines.Add("| $cache | " + ($cells -join " | ") + " |")
    }

    return $lines
}

function Build-DetailTableMarkdown($rows, $columns) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("| " + ($columns -join " | ") + " |")
    $lines.Add("|" + (($columns | ForEach-Object { "---" }) -join "|") + "|")
    foreach ($row in $rows) {
        $values = foreach ($col in $columns) {
            if ($col -eq "Method") {
                Format-MethodName $row.$col
            } else {
                $row.$col
            }
        }
        $lines.Add("| " + ($values -join " | ") + " |")
    }
    return $lines
}

function Add-Lines($target, $lines) {
    foreach ($line in $lines) {
        $target.Add([string]$line)
    }
}

Push-Location $repoRoot
try {
    if (-not $SkipRun) {
        dotnet run -c Release --project tests/Locus.Benchmarks --filter "Locus.Benchmarks.DirectoryQuotaDirtyFlushBenchmarks*"
        dotnet run -c Release --project tests/Locus.Benchmarks --filter "Locus.Benchmarks.KnownDirectoryTrimBenchmarks*"
    }

    if (-not (Test-Path $phase5Csv)) { throw "Missing benchmark result: $phase5Csv" }
    if (-not (Test-Path $phase6Csv)) { throw "Missing benchmark result: $phase6Csv" }

    $phase5Rows = Import-Csv $phase5Csv | Sort-Object { [int]$_.TotalDirectories }, { [int]$_.DirtyDirectories }
    $phase6Rows = Import-Csv $phase6Csv | Sort-Object { [int]$_.CacheMaxEntries }, { [int]$_.DirectoryAddsPerOperation }

    $phase5BaselineRow = $phase5Rows | Select-Object -First 1
    $phase6BaselineRow = $phase6Rows | Select-Object -First 1
    $phase5BaselineMs = Parse-MeanMilliseconds $phase5BaselineRow.Mean
    $phase6BaselineMs = Parse-MeanMilliseconds $phase6BaselineRow.Mean

    $phase5RowsWithRatio = $phase5Rows | ForEach-Object {
        $meanMs = Parse-MeanMilliseconds $_.Mean
        $ratio = if ($phase5BaselineMs -gt 0 -and -not [double]::IsNaN($meanMs)) {
            "{0:F2}x" -f ($meanMs / $phase5BaselineMs)
        } else {
            "N/A"
        }

        [PSCustomObject]@{
            TotalDirectories = $_.TotalDirectories
            DirtyDirectories = $_.DirtyDirectories
            Mean = $_.Mean
            RatioVsBaseline = $ratio
            Error = $_.Error
            StdDev = $_.StdDev
            Allocated = $_.Allocated
        }
    }

    $phase6RowsWithRatio = $phase6Rows | ForEach-Object {
        $meanMs = Parse-MeanMilliseconds $_.Mean
        $ratio = if ($phase6BaselineMs -gt 0 -and -not [double]::IsNaN($meanMs)) {
            "{0:F2}x" -f ($meanMs / $phase6BaselineMs)
        } else {
            "N/A"
        }

        [PSCustomObject]@{
            CacheMaxEntries = $_.CacheMaxEntries
            DirectoryAddsPerOperation = $_.DirectoryAddsPerOperation
            Mean = $_.Mean
            RatioVsBaseline = $ratio
            Error = $_.Error
            StdDev = $_.StdDev
            Allocated = $_.Allocated
        }
    }

    $content = New-Object System.Collections.Generic.List[string]
    $content.Add("# Phase 5/6 Matrix Panel")
    $content.Add("")
    $content.Add("Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')")
    $content.Add("")
    $content.Add("## Phase 5 - DirectoryQuota Dirty Flush (Mean)")
    $content.Add("")
    $content.Add("Baseline: TotalDirectories=$($phase5BaselineRow.TotalDirectories), DirtyDirectories=$($phase5BaselineRow.DirtyDirectories), Mean=$($phase5BaselineRow.Mean)")
    $content.Add("")
    Add-Lines $content (Build-Phase5MatrixMarkdown $phase5Rows)
    $content.Add("")
    $content.Add("### Phase 5 Details")
    $content.Add("")
    Add-Lines $content (Build-DetailTableMarkdown $phase5RowsWithRatio @("TotalDirectories","DirtyDirectories","Mean","RatioVsBaseline","Error","StdDev","Allocated"))
    $content.Add("")
    $content.Add("## Phase 6 - Known Directory Trim (Mean)")
    $content.Add("")
    $content.Add("Baseline: CacheMaxEntries=$($phase6BaselineRow.CacheMaxEntries), DirectoryAddsPerOperation=$($phase6BaselineRow.DirectoryAddsPerOperation), Mean=$($phase6BaselineRow.Mean)")
    $content.Add("")
    Add-Lines $content (Build-Phase6MatrixMarkdown $phase6Rows)
    $content.Add("")
    $content.Add("### Phase 6 Details")
    $content.Add("")
    Add-Lines $content (Build-DetailTableMarkdown $phase6RowsWithRatio @("CacheMaxEntries","DirectoryAddsPerOperation","Mean","RatioVsBaseline","Error","StdDev","Allocated"))
    $content.Add("")
    $content.Add("## Source Files")
    $content.Add("")
    $content.Add("- $phase5Csv")
    $content.Add("- $phase6Csv")

    Set-Content -Path $panelPath -Value $content -Encoding UTF8
    Write-Host "Generated panel: $panelPath"
}
finally {
    Pop-Location
}
