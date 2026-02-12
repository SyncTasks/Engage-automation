# Engage応募チェック - タスクスケジューラ登録スクリプト
# 管理者権限のPowerShellで実行してください

$taskName = "Engage応募チェック"
$basePath = "C:\Users\Administrator\Documents\GitHub\Engage-automation\ムームードメイン応募転記"
$batPath  = Join-Path $basePath "実行.bat"

# 既存タスクがあれば削除
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "[更新] 既存タスク '$taskName' を削除しました" -ForegroundColor Yellow
}

# トリガー: 毎日0:00開始、1時間ごとに繰り返し（無期限）
$trigger = New-ScheduledTaskTrigger -Daily -At "00:00"
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At "00:00" -RepetitionInterval (New-TimeSpan -Hours 1)).Repetition

# 操作: 実行.bat を実行
$action = New-ScheduledTaskAction -Execute $batPath -WorkingDirectory $basePath

# 設定
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

# 登録（SYSTEM権限で実行 = ログオフ中も動作）
Register-ScheduledTask `
    -TaskName $taskName `
    -Trigger $trigger `
    -Action $action `
    -Settings $settings `
    -User "SYSTEM" `
    -RunLevel Highest `
    -Description "Engageからの応募メールを1時間ごとにチェックし、スプレッドシートに転記・Chatwork通知"

Write-Host ""
Write-Host "[完了] タスク '$taskName' を登録しました" -ForegroundColor Green
Write-Host "  実行間隔: 1時間ごと" -ForegroundColor Cyan
Write-Host "  実行ファイル: $batPath" -ForegroundColor Cyan
Write-Host ""

# 確認表示
Get-ScheduledTask -TaskName $taskName | Format-List TaskName, State, Description
