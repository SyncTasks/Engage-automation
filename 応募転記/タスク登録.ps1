# エンゲージ応募転記 - タスクスケジューラ登録スクリプト
# 管理者権限のPowerShellで実行してください

$taskName = "エンゲージ応募転記"
$basePath = Split-Path -Parent $MyInvocation.MyCommand.Path
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
    -ExecutionTimeLimit (New-TimeSpan -Minutes 55)

# パスワード入力（最大3回リトライ）
$maxRetry = 3
$registered = $false

for ($i = 1; $i -le $maxRetry; $i++) {
    $password = Read-Host "Administratorのパスワードを入力してください ($i/$maxRetry)" -AsSecureString
    $plainPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($password))

    try {
        Register-ScheduledTask `
            -TaskName $taskName `
            -Trigger $trigger `
            -Action $action `
            -Settings $settings `
            -User "$env:COMPUTERNAME\$env:USERNAME" `
            -Password $plainPassword `
            -RunLevel Highest `
            -Description "エンゲージの応募者を1時間ごとにチェックし、スプレッドシートに転記・通知送信" `
            -ErrorAction Stop | Out-Null

        $registered = $true
        break
    } catch {
        Write-Host "[エラー] パスワードが正しくありません" -ForegroundColor Red
    }
}

if (-not $registered) {
    Write-Host ""
    Write-Host "[失敗] タスク登録に失敗しました（パスワード$maxRetry 回不正）" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "[完了] タスク '$taskName' を登録しました" -ForegroundColor Green
Write-Host "  実行間隔: 1時間ごと" -ForegroundColor Cyan
Write-Host "  実行ファイル: $batPath" -ForegroundColor Cyan
Write-Host ""

# 確認表示
Get-ScheduledTask -TaskName $taskName | Format-List TaskName, State, Description
