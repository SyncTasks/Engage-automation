$taskName = "Engage即時反応チェック"
$basePath = "C:\Users\Administrator\Documents\GitHub\Engage-automation\応募通知"
$batPath = Join-Path $basePath "即時反応.bat"

if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "[更新] 既存タスク削除しました" -ForegroundColor Yellow
}

$trigger = New-ScheduledTaskTrigger -Daily -At "00:00"
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At "00:00" -RepetitionInterval (New-TimeSpan -Minutes 1)).Repetition

$action = New-ScheduledTaskAction -Execute $batPath -WorkingDirectory $basePath

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5)

Register-ScheduledTask `
    -TaskName $taskName `
    -Trigger $trigger `
    -Action $action `
    -Settings $settings `
    -User "SYSTEM" `
    -RunLevel Highest `
    -Description "即時反応クライアントのEngage応募を1分ごとにチェック"

Write-Host ""
Write-Host "[完了] タスク登録しました" -ForegroundColor Green
Write-Host "  実行間隔: 1分ごと" -ForegroundColor Cyan
Write-Host "  実行ファイル: $batPath" -ForegroundColor Cyan
