# Windows 작업 스케줄러: 거래일 평일 17:00 일일 주식 분석
# 관리자 권한 없이 현재 사용자 계정으로 등록합니다.

$ErrorActionPreference = "Stop"
$ProjectRoot = $PSScriptRoot
$TaskName = "StockProgram-DailyAnalysis"

$python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    $python = (Get-Command python -ErrorAction SilentlyContinue).Source
}
if (-not $python) {
    throw "Python을 찾을 수 없습니다. .venv를 만들거나 PATH에 python을 추가하세요."
}

$runBatch = Join-Path $ProjectRoot "run_daily_task.cmd"

$action = New-ScheduledTaskAction `
    -Execute $runBatch `
    -WorkingDirectory $ProjectRoot

# 월~금 17:00 (휴장일은 run_scheduled_analysis.py에서 건너뜀)
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "17:00"

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6)

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Write-Host "등록 완료: $TaskName"
Write-Host "  Python: $python"
Write-Host "  실행: 월~금 17:00 -> run_daily_task.cmd (로그: logs\)"
Write-Host ""
Write-Host "확인: taskschd.msc 또는 Get-ScheduledTask -TaskName $TaskName"
Write-Host "삭제: Unregister-ScheduledTask -TaskName $TaskName -Confirm:`$false"
