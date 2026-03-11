# reCAPTCHA対策ガイド

## 概要
エンゲージ管理画面（en-gage.net）へのPlaywright自動ログイン時にreCAPTCHAが発生する問題と、その対策をまとめる。

---

## 原因と対策の全体像

| 原因 | 影響度 | 対策 |
|------|--------|------|
| User-Agentの不一致 | 高 | 偽UAを削除し、Chrome本体のUAを使用 |
| Chromiumの使用 | 高 | `channel="chrome"` でインストール済みChromeを使用 |
| 自動操作の検出 | 中 | playwright-stealth を適用 |
| データセンターIP | 最高 | 住宅回線で実行 or CSV方式に切替 |

---

## 対策1: User-Agent不一致の解消（必須）

### 問題
Playwrightのデフォルト or 手動設定のUser-Agentと、実際のChromeバージョンが不一致だとreCAPTCHAが発生する。

例: 実際はChrome/131なのに `Chrome/120` というUAを送信 → Google側で不審と判定

### 対策
`launch_persistent_context` や `launch` のオプションで `user_agent` を**指定しない**。
Chromeが自身の正しいUAを自動送信する。

```python
# NG: 偽のUAを指定
context = await playwright.chromium.launch_persistent_context(
    profile_dir,
    user_agent='Mozilla/5.0 ... Chrome/120 ...',  # ← これが原因
)

# OK: UAを指定しない（Chrome本体のUAが使われる）
context = await playwright.chromium.launch_persistent_context(
    profile_dir,
    channel="chrome",
    # user_agent は指定しない
)
```

### 確認方法
ブラウザで `chrome://version` を開き、User Agentの欄を確認。
コード側で送信しているUAと一致しているか確認する。

---

## 対策2: Chrome本体を使用（必須）

### 問題
Playwrightのバンドル版Chromium はreCAPTCHAに検知されやすい。

### 対策
`channel="chrome"` を指定して、PCにインストール済みのGoogle Chromeを使用する。

```python
context = await playwright.chromium.launch_persistent_context(
    profile_dir,
    headless=False,
    channel="chrome",  # ← インストール済みChromeを使用
)
```

### Chromeのインストール
- **Mac**: https://www.google.com/chrome/ からダウンロード
- **Windows VPS**: 以下のPowerShellコマンドで最新版をインストール
  ```powershell
  # Chromeインストーラをダウンロードして実行
  $installer = "$env:TEMP\ChromeSetup.exe"
  Invoke-WebRequest -Uri "https://dl.google.com/chrome/install/latest/chrome_installer.exe" -OutFile $installer
  Start-Process -FilePath $installer -Args "/silent /install" -Wait
  Remove-Item $installer
  ```

### Chromeを常に最新に保つ
古いChromeはreCAPTCHAリスクが上がる。
VPSでは自動更新が効かない場合があるため、定期的に手動更新するか、
タスクスケジューラで更新チェックを設定する。

```powershell
# Chrome更新チェック（Windows）
& "C:\Program Files\Google\Chrome\Application\chrome.exe" --check-for-update-interval=0
```

---

## 対策3: playwright-stealth の適用（必須）

### 問題
Playwrightは `navigator.webdriver = true` などの自動操作フラグを残す。

### 対策
`playwright-stealth` パッケージで自動操作の痕跡を隠す。

```bash
pip install playwright-stealth
```

```python
from playwright_stealth import Stealth

# ページ作成後に適用
page = context.pages[0] if context.pages else await context.new_page()
stealth = Stealth()
await stealth.apply_stealth_async(page)
```

### 注意点
- `stealth_async` という関数は存在しない（古い情報に注意）
- `Stealth().new_page()` も存在しない
- 正しいAPIは `Stealth().apply_stealth_async(page)`

---

## 対策4: 永続プロファイル（推奨）

### 問題
毎回新しいブラウザプロファイルだと、Cookie/履歴がなく不審に見える。

### 対策
`launch_persistent_context` でプロファイルを永続化する。

```python
profile_dir = os.path.join(CURRENT_DIR, "chrome_profile")

context = await playwright.chromium.launch_persistent_context(
    profile_dir,       # ← ここにCookie等が保存される
    headless=False,
    channel="chrome",
    locale='ja-JP',
    timezone_id='Asia/Tokyo',
    args=[
        '--disable-blink-features=AutomationControlled',
        '--no-sandbox',
        '--disable-dev-shm-usage',
    ]
)
```

---

## 対策5: データセンターIPの問題（VPS固有）

### 問題
VPS（AWS, Azure, さくら, ConoHa等）のIPアドレスはデータセンター帯域として知られており、
GoogleのreCAPTCHAはこれを高リスクと判定する。
**上記の対策1〜4をすべて実施しても、VPSではreCAPTCHAが出る可能性が高い。**

### 検証結果（2026年2月）
- Mac（住宅回線）: 対策1〜4で reCAPTCHA **解消**
- Windows VPS（データセンターIP）: 対策1〜4すべて実施しても reCAPTCHA **解消せず**

### 対策オプション

#### A. CSV転記方式に切替（推奨・採用済み）
ログイン自体を不要にする。エンゲージからCSVをエクスポートし、
GASでスプレッドシートに転記する方式。
→ `engage_notification/csv転記/` を参照

#### B. 住宅回線で実行
- Mac等のローカルPCで実行する
- VPSからMacにSSHトンネルを張り、SOCKSプロキシ経由でアクセスする
  ```bash
  # Mac側でSSHサーバを起動
  # VPS側から接続
  ssh -D 1080 user@mac-ip
  ```
  Playwrightの `proxy` オプションで `socks5://localhost:1080` を指定

#### C. Residential Proxy（住宅用プロキシ）
住宅IPを提供するプロキシサービスを利用する。
- Bright Data
- Oxylabs
- Smartproxy

```python
context = await playwright.chromium.launch_persistent_context(
    profile_dir,
    channel="chrome",
    proxy={"server": "http://proxy-server:port", "username": "user", "password": "pass"},
)
```

#### D. reCAPTCHA解決サービス
2Captchaなどのサービスで自動解決する。コスト：1000回あたり約$3。
```python
# 2Captcha APIの例
import requests
API_KEY = 'your_2captcha_key'
# サイトキーを取得してAPIに送信 → トークンを受け取り → フォームに注入
```

---

## 現在のコードの対策状況

`engage_check_apply.py` に実装済みの対策：

| 対策 | 状態 |
|------|------|
| Chrome本体使用 (`channel="chrome"`) | 実装済み |
| 偽User-Agent削除 | 実装済み |
| playwright-stealth | 実装済み |
| 永続プロファイル | 実装済み |
| `--disable-blink-features=AutomationControlled` | 実装済み |
| 手動ログイン方式（ID/パスワード自動入力のみ） | 実装済み |
| ヒューマンライクな遅延・タイピング・マウス操作 | 実装済み |

---

## トラブルシューティング

### reCAPTCHAが出る場合のチェックリスト

1. **Chromeは最新か？**
   - `chrome://version` で確認
   - 古い場合はアップデート

2. **User-Agentを手動指定していないか？**
   - `user_agent` パラメータを削除

3. **`channel="chrome"` を指定しているか？**
   - 未指定だとバンドルChromiumが使われる

4. **playwright-stealth を適用しているか？**
   - `Stealth().apply_stealth_async(page)` を呼んでいるか

5. **VPS（データセンターIP）ではないか？**
   - VPSの場合、クライアントサイドの対策だけでは不十分
   - CSV方式への切替を推奨

6. **headless=True にしていないか？**
   - ヘッドレスモードはreCAPTCHAに検知されやすい
   - `headless=False` を使用する

7. **Chrome以外のブラウザを使っていないか？**
   - FirefoxやWebKitはreCAPTCHA回避に不向き
