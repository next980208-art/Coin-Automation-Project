param(
    [string]$OutputDirectory = "assignment_evidence"
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$outputPath = Join-Path $projectRoot $OutputDirectory
$assetPath = Join-Path $outputPath "assets"
New-Item -ItemType Directory -Path $assetPath -Force | Out-Null

Add-Type -AssemblyName System.Drawing

function Save-JsonUtf8 {
    param([object]$Value, [string]$Path)
    $Value | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $Path -Encoding UTF8
}

function Get-StatusColor {
    param([string]$Value)
    if ($Value -match "healthy|success|FINISHED|0건|true") {
        return [System.Drawing.Color]::FromArgb(32, 125, 78)
    }
    if ($Value -match "failed|error|누락|중복 [1-9]") {
        return [System.Drawing.Color]::FromArgb(184, 50, 45)
    }
    return [System.Drawing.Color]::FromArgb(32, 67, 113)
}

function New-EvidenceImage {
    param(
        [string]$Path,
        [string]$Title,
        [string]$Subtitle,
        [array]$Groups,
        [string]$Footer
    )

    $rowCount = 0
    foreach ($group in $Groups) { $rowCount += $group.Rows.Count }
    $height = 250 + ($Groups.Count * 62) + ($rowCount * 58) + 90
    $width = 1600
    $bitmap = New-Object System.Drawing.Bitmap($width, $height)
    $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
    $graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $graphics.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::AntiAliasGridFit
    $graphics.Clear([System.Drawing.Color]::FromArgb(248, 249, 250))

    $titleFont = New-Object System.Drawing.Font("Malgun Gothic", 30, [System.Drawing.FontStyle]::Bold)
    $subtitleFont = New-Object System.Drawing.Font("Malgun Gothic", 14)
    $groupFont = New-Object System.Drawing.Font("Malgun Gothic", 18, [System.Drawing.FontStyle]::Bold)
    $labelFont = New-Object System.Drawing.Font("Malgun Gothic", 14)
    $valueFont = New-Object System.Drawing.Font("Malgun Gothic", 14, [System.Drawing.FontStyle]::Bold)
    $footerFont = New-Object System.Drawing.Font("Malgun Gothic", 11)

    $headerBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(31, 35, 40))
    $whiteBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::White)
    $mutedBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(210, 215, 222))
    $groupBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(31, 35, 40))
    $labelBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(72, 79, 88))
    $rowBrushA = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::White)
    $rowBrushB = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(239, 243, 247))
    $footerBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(92, 99, 108))
    $linePen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(210, 215, 222), 1)

    $graphics.FillRectangle($headerBrush, 0, 0, $width, 185)
    $graphics.DrawString($Title, $titleFont, $whiteBrush, 70, 38)
    $graphics.DrawString($Subtitle, $subtitleFont, $mutedBrush, 72, 105)
    $graphics.DrawString("실제 실행 데이터 기반 증거", $subtitleFont, $mutedBrush, 72, 140)

    $y = 215
    $rowIndex = 0
    foreach ($group in $Groups) {
        $graphics.DrawString($group.Name, $groupFont, $groupBrush, 72, $y)
        $y += 46
        foreach ($row in $group.Rows) {
            $rowBrush = if (($rowIndex % 2) -eq 0) { $rowBrushA } else { $rowBrushB }
            $graphics.FillRectangle($rowBrush, 70, $y, 1460, 52)
            $graphics.DrawRectangle($linePen, 70, $y, 1460, 52)
            $graphics.DrawString([string]$row.Label, $labelFont, $labelBrush, 92, $y + 12)
            $valueColor = Get-StatusColor ([string]$row.Value)
            $valueBrush = New-Object System.Drawing.SolidBrush($valueColor)
            $valueRect = New-Object System.Drawing.RectangleF -ArgumentList 555, ($y + 7), 945, 40
            $graphics.DrawString([string]$row.Value, $valueFont, $valueBrush, $valueRect)
            $valueBrush.Dispose()
            $y += 58
            $rowIndex++
        }
        $y += 16
    }

    $graphics.DrawString($Footer, $footerFont, $footerBrush, 72, $height - 58)
    $bitmap.Save($Path, [System.Drawing.Imaging.ImageFormat]::Png)

    foreach ($item in @(
        $titleFont, $subtitleFont, $groupFont, $labelFont, $valueFont, $footerFont,
        $headerBrush, $whiteBrush, $mutedBrush, $groupBrush, $labelBrush,
        $rowBrushA, $rowBrushB, $footerBrush, $linePen, $graphics, $bitmap
    )) { $item.Dispose() }
}

$airflowHealth = Invoke-RestMethod -Uri "http://localhost:8080/health"
$flinkOverview = Invoke-RestMethod -Uri "http://localhost:8081/overview"
$flinkJobs = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview"

$airflowRunsRaw = & docker compose exec -T airflow airflow dags list-runs `
    -d btcusdt_usdm_historical_backfill --output json
if ($LASTEXITCODE -ne 0) { throw "Airflow DAG run 조회 실패" }
$airflowRuns = $airflowRunsRaw | ConvertFrom-Json
$assignmentRun = $airflowRuns | Where-Object {
    $_.run_id -eq "manual__2026-08-26T14:00:00+00:00"
} | Select-Object -First 1
if ($null -eq $assignmentRun) { throw "과제 Airflow DAG run을 찾지 못했습니다." }

$assignment4 = Get-Content -Raw -Encoding UTF8 -LiteralPath (
    Join-Path $projectRoot "docs/airflow_parameterized_backfill_run_2026-08-26.json"
) | ConvertFrom-Json
$assignment5 = Get-Content -Raw -Encoding UTF8 -LiteralPath (
    Join-Path $projectRoot "assignment5_pipeline_resilience/results/assignment5_final_report.json"
) | ConvertFrom-Json
$quality = Get-Content -Raw -Encoding UTF8 -LiteralPath (
    Join-Path $projectRoot "assignment5_pipeline_resilience/results/assignment5_output_quality_check.json"
) | ConvertFrom-Json

Save-JsonUtf8 $airflowHealth (Join-Path $outputPath "airflow_live_health.json")
Save-JsonUtf8 $airflowRuns (Join-Path $outputPath "airflow_dag_runs_snapshot.json")
Save-JsonUtf8 $flinkOverview (Join-Path $outputPath "flink_live_overview.json")
Save-JsonUtf8 $flinkJobs (Join-Path $outputPath "flink_jobs_snapshot.json")

$baseline = $assignment5.scenarios | Where-Object { $_.name -eq "baseline_1000" }
$load = $assignment5.scenarios | Where-Object { $_.name -eq "load_10000" }
$baselineJob = $flinkJobs.jobs | Where-Object { $_.jid -eq $baseline.flink.flink_job_id }
$loadJob = $flinkJobs.jobs | Where-Object { $_.jid -eq $load.flink.flink_job_id }
$baselineQuality = $quality.scenarios.baseline_1000
$loadQuality = $quality.scenarios.load_10000

$airflowGroups = @(
    [PSCustomObject]@{
        Name = "Airflow 현재 상태"
        Rows = @(
            @{ Label = "Metadatabase"; Value = $airflowHealth.metadatabase.status },
            @{ Label = "Scheduler"; Value = $airflowHealth.scheduler.status },
            @{ Label = "Triggerer"; Value = $airflowHealth.triggerer.status }
        )
    },
    [PSCustomObject]@{
        Name = "파라미터 변경 실제 실행"
        Rows = @(
            @{ Label = "DAG Run ID"; Value = $assignmentRun.run_id },
            @{ Label = "Airflow DB 상태"; Value = $assignmentRun.state },
            @{ Label = "입력값"; Value = "$($assignment4.input.symbol), $($assignment4.input.start_date) ~ $($assignment4.input.end_date)" },
            @{ Label = "수집·저장"; Value = "OHLCV $($assignment4.raw_ohlcv_rows_downloaded)행 / Feature $($assignment4.feature_store.rows)행 / Context $($assignment4.futures_context_store.rows)행" },
            @{ Label = "품질"; Value = "중복 $($assignment4.feature_store.duplicate_timestamps)건 / 1분 공백 $($assignment4.feature_store.one_minute_gaps)건 / healthy=$($assignment4.feature_store.healthy)" },
            @{ Label = "PyFlink Job ID"; Value = $assignment4.flink_batch_job_id }
        )
    }
)
New-EvidenceImage `
    -Path (Join-Path $assetPath "01_airflow_actual_run.png") `
    -Title "Airflow 파라미터 백필 실제 실행" `
    -Subtitle "BTC 고정 코드를 수정하지 않고 ETHUSDT와 날짜 범위를 입력해 실행" `
    -Groups $airflowGroups `
    -Footer "출처: Airflow /health, Airflow metadata DB, airflow_parameterized_backfill_run_2026-08-26.json"

$flinkGroups = @(
    [PSCustomObject]@{
        Name = "Flink 클러스터 현재 상태"
        Rows = @(
            @{ Label = "Flink Version"; Value = $flinkOverview."flink-version" },
            @{ Label = "TaskManagers / Slots"; Value = "$($flinkOverview.taskmanagers) / $($flinkOverview.'slots-total')" },
            @{ Label = "Finished / Failed Jobs"; Value = "$($flinkOverview.'jobs-finished') / $($flinkOverview.'jobs-failed')" }
        )
    },
    [PSCustomObject]@{
        Name = "과제 PyFlink Job"
        Rows = @(
            @{ Label = "정상량 Job ID"; Value = $baseline.flink.flink_job_id },
            @{ Label = "정상량 API 상태"; Value = "$($baselineJob.state), tasks $($baselineJob.tasks.finished)/$($baselineJob.tasks.total) finished" },
            @{ Label = "부하량 Job ID"; Value = $load.flink.flink_job_id },
            @{ Label = "부하량 API 상태"; Value = "$($loadJob.state), tasks $($loadJob.tasks.finished)/$($loadJob.tasks.total) finished" }
        )
    }
)
New-EvidenceImage `
    -Path (Join-Path $assetPath "02_flink_completed_jobs.png") `
    -Title "Apache Flink 실제 완료 Job" `
    -Subtitle "Flink REST API에서 정상량·부하량 Job ID와 FINISHED 상태를 다시 조회" `
    -Groups $flinkGroups `
    -Footer "출처: http://localhost:8081/overview, /jobs/overview"

$loadGroups = @(
    [PSCustomObject]@{
        Name = "정상 입력량"
        Rows = @(
            @{ Label = "Kafka Producer / Consumer"; Value = "$($baseline.producer.producer_sent_count)건 / $($baseline.consumer.consumer_received_count)건" },
            @{ Label = "Kafka 구간"; Value = "$($baseline.kafka_end_to_end_seconds)초" },
            @{ Label = "PyFlink 입력 / 출력"; Value = "$($baseline.flink.flink_input_valid_count)행 / $($baseline.flink.flink_output_processed_count)행" },
            @{ Label = "전체 시간"; Value = "$($baseline.total_pipeline_seconds)초" }
        )
    },
    [PSCustomObject]@{
        Name = "부하 입력량"
        Rows = @(
            @{ Label = "Kafka 총 전송"; Value = "$($load.producer.producer_sent_count)건 (고유 $($load.expected_unique_count) + 중복 $($load.intentional_duplicate_count))" },
            @{ Label = "Consumer 중복 감지"; Value = "$($load.consumer.duplicate_message_count)건" },
            @{ Label = "PyFlink 입력 / 출력"; Value = "$($load.flink.flink_input_valid_count)행 / $($load.flink.flink_output_processed_count)행" },
            @{ Label = "전체 시간"; Value = "$($load.total_pipeline_seconds)초" }
        )
    }
)
New-EvidenceImage `
    -Path (Join-Path $assetPath "03_kafka_flink_load_comparison.png") `
    -Title "Kafka·PyFlink 정상량 대 부하량" `
    -Subtitle "저장된 실제 Binance 이벤트를 외부 요청 없이 로컬 Kafka에서 재생" `
    -Groups $loadGroups `
    -Footer "주의: 10,000건은 실제 1,000건 패턴을 로컬에서 고유 timestamp·event_id로 확장한 재생 데이터"

$fault = $assignment5.fault_and_recovery
$recoveryGroups = @(
    [PSCustomObject]@{
        Name = "장애 재현"
        Rows = @(
            @{ Label = "중복 장애"; Value = "500건 전송 / 500건 감지" },
            @{ Label = "잘못된 입력"; Value = "필수 close 필드 제거" },
            @{ Label = "실패 여부 / 종료 코드"; Value = "$($fault.failure_reproduced) / $($fault.process_return_code)" },
            @{ Label = "실제 오류"; Value = $fault.error_tail }
        )
    },
    [PSCustomObject]@{
        Name = "복구 및 저장 품질"
        Rows = @(
            @{ Label = "복구 출력 / 중복"; Value = "$($fault.recovery_output_rows)행 / $($fault.recovery_duplicate_rows)건" },
            @{ Label = "정상량 Parquet"; Value = "$($baselineQuality.parquet_rows)행 / 중복 $($baselineQuality.duplicate_timestamps)건 / 결측 $($baselineQuality.missing_required_values)건" },
            @{ Label = "부하량 Parquet"; Value = "$($loadQuality.parquet_rows)행 / 중복 $($loadQuality.duplicate_timestamps)건 / 결측 $($loadQuality.missing_required_values)건" },
            @{ Label = "최종 자동 판정"; Value = "healthy=$($assignment5.validation.healthy), errors=$($assignment5.validation.errors.Count)" }
        )
    }
)
New-EvidenceImage `
    -Path (Join-Path $assetPath "04_fault_recovery_quality.png") `
    -Title "장애 재현·복구·Parquet 검증" `
    -Subtitle "중복 이벤트와 필수 필드 누락을 안전하게 재현한 뒤 정상 입력으로 복구" `
    -Groups $recoveryGroups `
    -Footer "출처: assignment5_final_report.json, assignment5_output_quality_check.json, fault_invalid_input.log"

Write-Output "실제 실행 증거 파일을 생성했습니다: $outputPath"
Get-ChildItem -LiteralPath $assetPath -File | Select-Object Name, Length
