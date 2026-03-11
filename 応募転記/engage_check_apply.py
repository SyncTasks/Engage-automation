import asyncio
import re
import random
import time
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, parse_qs
from dataclasses import dataclass
import os
import certifi
import json
from datetime import datetime
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from playwright.async_api import async_playwright, Page, ElementHandle, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth
from SpreadsheetManager import SpreadsheetManager, write_to_spreadsheet
from Notification.NotificationManagerClass import NotificationManager
from Notification.config import config
from constants import AREA_MAPPING, FACILITY_TYPES, PROFESSIONS

os.environ['SSL_CERT_FILE'] = certifi.where()
load_dotenv()

# TalentDB関連の設定
SPREADSHEET_ID = os.getenv("TALENT_DB_SPREADSHEET_ID")
MEDIA_NAME = "エンゲージ"

# 2Captcha設定
TWOCAPTCHA_API_KEY = '510106e415b0e9081f74199608d3b5c2'

# 実行履歴スプレッドシート設定
EXECUTION_LOG_SPREADSHEET_ID = '1j-u3vJ0DaJLKoeF1F_MR6eywWU07XDg_rAOblMjktlk'
EXECUTION_LOG_SHEET_NAME = '実行履歴'

# ログファイル設定
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE_PREFIX = "engage_scraper_"
LOG_FILE_EXTENSION = ".log"

def get_log_file_path():
    """当日の日付を含むログファイルパスを取得"""
    today = datetime.now().strftime('%Y-%m-%d')
    return os.path.join(CURRENT_DIR, f"{LOG_FILE_PREFIX}{today}{LOG_FILE_EXTENSION}")

def cleanup_old_logs():
    """1日より古いログファイルを削除"""
    import glob
    from datetime import timedelta

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    pattern = os.path.join(CURRENT_DIR, f"{LOG_FILE_PREFIX}*{LOG_FILE_EXTENSION}")

    for log_file in glob.glob(pattern):
        filename = os.path.basename(log_file)
        # ファイル名から日付を抽出（例: engage_scraper_2026-01-29.log → 2026-01-29）
        try:
            date_str = filename.replace(LOG_FILE_PREFIX, "").replace(LOG_FILE_EXTENSION, "")
            if date_str < yesterday:
                os.remove(log_file)
                print(f"古いログファイルを削除: {filename}")
        except Exception:
            pass

def init_log_file():
    """ログファイルを初期化（古いログを削除、当日のログに追記）"""
    cleanup_old_logs()
    log_path = get_log_file_path()
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(f"\n=== Engage Scraper Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n\n")

def write_log(message: str) -> None:
    """ログをファイルに書き込む"""
    try:
        log_path = get_log_file_path()
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(message + '\n')
    except Exception:
        pass  # ファイル書き込みエラーは無視

# スクリプト起動時にログファイルを初期化
init_log_file()

@dataclass
class User:
    media_name: str
    client_name: str
    user_id: str
    password: str
    is_active: bool = True

@dataclass
class ExecutionRecord:
    """1クライアントの実行履歴を記録するデータクラス"""
    start_time: str = ''
    client_name: str = ''
    login_result: str = ''          # 成功 / 失敗
    recaptcha_status: str = 'なし'  # なし / チェックボックス通過 / 2Captcha解決 / 解決失敗
    captcha_solve_time: str = ''    # 2Captcha解決時間（秒）
    new_applicants: int = 0         # 新規応募者数
    written_count: int = 0          # 転記成功数
    duplicate_count: int = 0        # 重複スキップ数
    error_message: str = ''         # エラー内容
    processing_time: str = ''       # 処理時間（秒）

    def to_row(self) -> list:
        return [
            self.start_time,
            self.client_name,
            self.login_result,
            self.recaptcha_status,
            self.captcha_solve_time,
            self.new_applicants,
            self.written_count,
            self.duplicate_count,
            self.error_message,
            self.processing_time,
        ]

class SpreadsheetUserRepository:
    def __init__(self, client: gspread.Client, spreadsheet_id: str, sheet_name: str):
        self.worksheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    def find_by_media(self, media_name: str) -> List[User]:
        all_records = self.worksheet.get_all_records()
        users = []
        for record in all_records:
            media = record.get('媒体名', '')
            # 媒体名が「エンゲージ」または「engage」（大文字小文字問わず）にマッチ
            if media == media_name or media.lower() == media_name.lower() or media.lower() == 'engage':
                # is_activeがTRUE/FALSE文字列またはブール値に対応
                is_active_value = record.get('is_active', True)
                if isinstance(is_active_value, str):
                    is_active = is_active_value.upper() == 'TRUE'
                else:
                    is_active = bool(is_active_value)

                users.append(User(
                    media_name=media,
                    client_name=record.get('クライアント名', ''),
                    user_id=record.get('メール', ''),
                    password=record.get('パス', ''),
                    is_active=is_active
                ))
        return users

def create_credentials() -> Optional[Dict[str, Any]]:
    try:
        with open('Credentials.json', 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print_log("Credentials.jsonファイルが見つかりません。")
        return None

def authorize_spreadsheet() -> Optional[gspread.Client]:
    credentials = create_credentials()
    if not credentials:
        return None
    
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials, scope)
    return gspread.authorize(creds)

def write_execution_log(record: ExecutionRecord) -> None:
    """実行履歴を Google スプレッドシートに書き込む"""
    try:
        client = authorize_spreadsheet()
        if not client:
            print_log("実行履歴: スプレッドシート認証に失敗しました")
            return

        spreadsheet = client.open_by_key(EXECUTION_LOG_SPREADSHEET_ID)
        sheet = spreadsheet.worksheet(EXECUTION_LOG_SHEET_NAME)

        # ヘッダーが未設定なら書き込む
        existing = sheet.row_values(1)
        if not existing:
            headers = [
                '実行日時', 'クライアント名', 'ログイン結果',
                'reCAPTCHA状態', '2Captcha解決時間(秒)',
                '新規応募者数', '転記成功数', '重複スキップ数',
                'エラー内容', '処理時間(秒)',
            ]
            sheet.append_row(headers, value_input_option='RAW')

        sheet.append_row(record.to_row(), value_input_option='USER_ENTERED')
        print_log(f"実行履歴を記録しました: {record.client_name}")
    except Exception as e:
        print_log(f"実行履歴の書き込みに失敗しました: {type(e).__name__}: {str(e)}")

def get_active_accounts() -> Optional[List[User]]:
    """認証DBからアクティブなアカウントを取得する"""
    try:
        client = authorize_spreadsheet()
        if not client:
            print_log("スプレッドシートの認証に失敗しました。")
            return None

        user_repo = SpreadsheetUserRepository(
            client,
            SPREADSHEET_ID,
            "ユーザ"
        )

        users = user_repo.find_by_media(MEDIA_NAME)
        active_users = [user for user in users if user.is_active]
        print_log(f"{MEDIA_NAME}のアクティブユーザー数: {len(active_users)}")
        return active_users

    except Exception as e:
        print_log(f"エラーが発生しました: {str(e)}")
        return None

def print_log(message: str) -> None:
    """ログメッセージを出力する（コンソールとファイル両方に出力）"""
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    log_message = f"DEBUG [{current_time}]: {message}"
    print(log_message)
    write_log(log_message)

async def human_delay(min_ms: int = 500, max_ms: int = 1500) -> None:
    """人間らしいランダムな遅延を追加"""
    delay = random.randint(min_ms, max_ms) / 1000
    await asyncio.sleep(delay)

async def human_type(page: Page, selector: str, text: str) -> None:
    """人間らしいタイピング速度で入力"""
    await page.click(selector)
    await human_delay(100, 300)
    for char in text:
        await page.keyboard.type(char, delay=random.randint(50, 150))
    await human_delay(200, 500)

async def human_mouse_move(page: Page, x: int, y: int) -> None:
    """人間らしいマウス移動（カーブ付き）"""
    # 現在位置から目標位置まで複数ステップで移動
    steps = random.randint(3, 7)
    for i in range(steps):
        progress = (i + 1) / steps
        # ランダムな揺らぎを追加
        offset_x = random.randint(-10, 10)
        offset_y = random.randint(-10, 10)
        current_x = int(x * progress) + offset_x
        current_y = int(y * progress) + offset_y
        await page.mouse.move(current_x, current_y)
        await asyncio.sleep(random.uniform(0.01, 0.05))
    await page.mouse.move(x, y)

async def detect_recaptcha(page: Page) -> Optional[str]:
    """ページ上のreCAPTCHAが実際に表示されているか検出し、サイトキーを返す。非表示ならNone。"""
    sitekey = await page.evaluate('''() => {
        // reCAPTCHAのiframeが実際に表示されているかチェック
        const iframe = document.querySelector('iframe[src*="recaptcha/api2/anchor"]');
        if (!iframe) return null;

        // iframeまたはその親要素が非表示なら検出しない
        const rect = iframe.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) return null;

        // サイトキーを取得
        const el = document.querySelector('[data-sitekey]');
        if (el) return el.getAttribute('data-sitekey');

        const match = iframe.src.match(/[?&]k=([^&]+)/);
        if (match) return match[1];

        return null;
    }''')
    return sitekey


def solve_recaptcha_2captcha(sitekey: str, page_url: str) -> Optional[str]:
    """2Captcha APIでreCAPTCHA v2を解決し、トークンを返す。"""
    print_log(f"2Captcha: reCAPTCHA解決リクエスト送信中 (sitekey={sitekey[:16]}...)")

    # 1. 解決リクエスト送信
    try:
        resp = requests.post('http://2captcha.com/in.php', data={
            'key': TWOCAPTCHA_API_KEY,
            'method': 'userrecaptcha',
            'googlekey': sitekey,
            'pageurl': page_url,
            'json': 1,
        }, timeout=30)
        result = resp.json()
    except Exception as e:
        print_log(f"2Captcha: リクエスト送信エラー: {str(e)}")
        return None

    if result.get('status') != 1:
        print_log(f"2Captcha: リクエスト失敗: {result.get('request', 'unknown error')}")
        return None

    request_id = result['request']
    print_log(f"2Captcha: リクエストID={request_id}、解決待機中...")

    # 2. 結果をポーリング（最大180秒）
    for attempt in range(36):
        time.sleep(5)
        try:
            resp = requests.get('http://2captcha.com/res.php', params={
                'key': TWOCAPTCHA_API_KEY,
                'action': 'get',
                'id': request_id,
                'json': 1,
            }, timeout=30)
            result = resp.json()
        except Exception as e:
            print_log(f"2Captcha: ポーリングエラー: {str(e)}")
            continue

        if result.get('status') == 1:
            token = result['request']
            print_log(f"2Captcha: 解決成功（{(attempt + 1) * 5}秒）")
            return token

        if result.get('request') != 'CAPCHA_NOT_READY':
            print_log(f"2Captcha: エラー: {result.get('request', 'unknown')}")
            return None

    print_log("2Captcha: タイムアウト（180秒）")
    return None


async def click_recaptcha_checkbox(page: Page) -> bool:
    """reCAPTCHAのチェックボックス（iframe内）をクリックする。成功したらTrue。"""
    try:
        recaptcha_frame = page.frame_locator('iframe[src*="recaptcha/api2/anchor"]')
        checkbox = recaptcha_frame.locator('#recaptcha-anchor')
        await checkbox.click(timeout=5000)
        print_log("reCAPTCHAチェックボックスをクリックしました")
        await human_delay(2000, 3000)
        return True
    except Exception as e:
        print_log(f"reCAPTCHAチェックボックスのクリックに失敗: {str(e)}")
        return False


async def is_recaptcha_solved(page: Page) -> bool:
    """reCAPTCHAが解決済み（緑チェックマーク）かどうかを確認する。"""
    try:
        recaptcha_frame = page.frame_locator('iframe[src*="recaptcha/api2/anchor"]')
        checkbox = recaptcha_frame.locator('#recaptcha-anchor')
        aria_checked = await checkbox.get_attribute('aria-checked', timeout=3000)
        return aria_checked == 'true'
    except Exception:
        return False


async def is_recaptcha_expired(page: Page) -> bool:
    """reCAPTCHAが時間切れ状態かどうかを確認する。"""
    try:
        recaptcha_frame = page.frame_locator('iframe[src*="recaptcha/api2/anchor"]')
        checkbox = recaptcha_frame.locator('#recaptcha-anchor')
        class_attr = await checkbox.get_attribute('class', timeout=3000)
        if class_attr and 'recaptcha-checkbox-expired' in class_attr:
            print_log("reCAPTCHAが時間切れ状態です")
            return True
        return False
    except Exception:
        return False


async def inject_recaptcha_token(page: Page, token: str) -> None:
    """reCAPTCHAトークンをページに注入し、コールバックを呼び出す。"""
    callback_called = await page.evaluate('''(token) => {
        let callbackCalled = false;

        // 1. g-recaptcha-responseテキストエリアにトークンを設定
        document.querySelectorAll('[name="g-recaptcha-response"], #g-recaptcha-response').forEach(el => {
            el.style.display = 'block';
            el.value = token;
        });
        // iframe内のテキストエリアも設定
        document.querySelectorAll('textarea').forEach(el => {
            if (el.id && el.id.includes('g-recaptcha-response')) {
                el.innerHTML = token;
                el.value = token;
            }
        });

        // 2. grecaptcha.getResponse をオーバーライド（フォームバリデーション対策）
        if (typeof grecaptcha !== 'undefined') {
            grecaptcha.getResponse = function() { return token; };
        }

        // 3. コールバック関数を探して実行（複数パターン対応）

        // パターンA: data-callback属性から取得
        const recaptchaEl = document.querySelector('[data-callback]');
        if (recaptchaEl) {
            const cbName = recaptchaEl.getAttribute('data-callback');
            if (typeof window[cbName] === 'function') {
                window[cbName](token);
                callbackCalled = true;
            }
        }

        // パターンB: ___grecaptcha_cfg.clients から再帰的に探す
        if (typeof ___grecaptcha_cfg !== 'undefined' && ___grecaptcha_cfg.clients) {
            Object.values(___grecaptcha_cfg.clients).forEach(client => {
                const findCallback = (obj, depth) => {
                    if (depth > 5 || !obj) return;
                    for (const key of Object.keys(obj)) {
                        if (key === 'callback' && typeof obj[key] === 'function') {
                            obj[key](token);
                            callbackCalled = true;
                            return;
                        }
                        if (typeof obj[key] === 'object') {
                            findCallback(obj[key], depth + 1);
                        }
                    }
                };
                findCallback(client, 0);
            });
        }

        return callbackCalled;
    }''', token)
    print_log(f"reCAPTCHAトークン注入完了（コールバック呼出: {'成功' if callback_called else '未検出'}）")


async def is_challenge_visible(page: Page) -> bool:
    """reCAPTCHAの画像チャレンジ（bframe）が実際に表示されているか確認する。"""
    try:
        visible = await page.evaluate('''() => {
            const bframe = document.querySelector('iframe[src*="recaptcha/api2/bframe"]');
            if (!bframe) return false;
            const rect = bframe.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
        }''')
        return visible
    except Exception:
        return False


# reCAPTCHA処理の最新結果を保持（実行履歴記録用）
_last_recaptcha_status = 'なし'
_last_captcha_solve_time = ''

async def handle_recaptcha_if_present(page: Page) -> bool:
    """reCAPTCHAが検出された場合、チェックボックスクリック→必要なら2Captchaで解決。"""
    global _last_recaptcha_status, _last_captcha_solve_time
    _last_recaptcha_status = 'なし'
    _last_captcha_solve_time = ''

    sitekey = await detect_recaptcha(page)
    if not sitekey:
        return False

    print_log(f"reCAPTCHA検出！サイトキー: {sitekey[:16]}...")

    # 1. まずチェックボックスをクリック（これだけで通過する場合がある）
    clicked = await click_recaptcha_checkbox(page)
    if not clicked:
        print_log("チェックボックスクリック失敗、2Captchaで解決を試みます...")

    # 2. チェックボックスクリックで通過したか確認
    if await is_recaptcha_solved(page):
        print_log("reCAPTCHAチェックボックスクリックのみで通過しました！")
        _last_recaptcha_status = 'チェックボックス通過'
        return True

    # 3. 画像チャレンジが実際に表示されているか確認
    if not await is_challenge_visible(page):
        print_log("画像チャレンジは表示されていません。少し待機して再確認...")
        await human_delay(2000, 3000)
        # 再度チェック：待機中に解決した可能性
        if await is_recaptcha_solved(page):
            print_log("待機後にreCAPTCHA通過を確認しました！")
            _last_recaptcha_status = 'チェックボックス通過'
            return True
        # まだ解決せず、チャレンジも出ていない場合
        if not await is_challenge_visible(page):
            print_log("画像チャレンジが表示されていないため、チェックボックスを再クリック...")
            await click_recaptcha_checkbox(page)
            if await is_recaptcha_solved(page):
                print_log("再クリックでreCAPTCHA通過しました！")
                _last_recaptcha_status = 'チェックボックス通過'
                return True
            if not await is_challenge_visible(page):
                print_log("チャレンジが表示されません。reCAPTCHA解決失敗。")
                _last_recaptcha_status = '解決失敗'
                return False

    # 4. 画像チャレンジが表示されている → 2Captchaで解決
    print_log("画像チャレンジが表示されています。2Captchaで解決中...")
    page_url = page.url
    solve_start = time.time()

    token = await asyncio.get_event_loop().run_in_executor(
        None, solve_recaptcha_2captcha, sitekey, page_url
    )

    solve_elapsed = int(time.time() - solve_start)

    if not token:
        print_log("reCAPTCHA解決失敗")
        _last_recaptcha_status = '解決失敗'
        _last_captcha_solve_time = str(solve_elapsed)
        return False

    _last_captcha_solve_time = str(solve_elapsed)

    # 5. トークン注入前にreCAPTCHAが時間切れしていないか確認
    if await is_recaptcha_expired(page):
        print_log("reCAPTCHAが時間切れのため、チェックボックスを再クリックしてリセット...")
        await click_recaptcha_checkbox(page)
        await human_delay(500, 1000)

    # 6. トークンを注入
    await inject_recaptcha_token(page, token)
    print_log("reCAPTCHAトークン注入完了")
    await human_delay(500, 1000)
    _last_recaptcha_status = '2Captcha解決'
    return True


async def wait_for_element(page: Page, selector: str, timeout: int = 10000) -> Optional[ElementHandle]:
    """指定されたセレクタの要素が表示されるまで待機する"""
    try:
        return await page.wait_for_selector(selector, state='visible', timeout=timeout)
    except PlaywrightTimeoutError:
        print_log(f"要素が見つかりません: {selector}")
        return None

async def close_modal_if_exists(page: Page) -> bool:
    """モーダルが存在すればJavaScriptで強制的に非表示にする（クリックによるチェーン発動を回避）"""
    try:
        result = await page.evaluate('''() => {
            let closedCount = 0;
            // 表示中のモーダルをすべて強制非表示（クリックせずDOM操作のみ）
            document.querySelectorAll('.md_modal--show').forEach(modal => {
                modal.classList.remove('md_modal--show');
                modal.style.display = 'none';
                closedCount++;
            });
            // body のスクロールロックも解除（モーダル表示時にbodyにoverflow:hiddenが付く場合がある）
            document.body.style.overflow = '';
            document.body.classList.remove('md_modal--open');
            return closedCount;
        }''')
        if result > 0:
            print_log(f"モーダルを{result}件、JavaScriptで強制的に閉じました")
            await human_delay(200, 400)
            return True
    except Exception as e:
        print_log(f"モーダル強制非表示エラー: {str(e)}")

    return False

async def get_element_text(element: ElementHandle, selector: str) -> str:
    """要素から指定されたセレクタのテキストを取得する"""
    try:
        target_element = await element.query_selector(selector)
        return await target_element.inner_text() if target_element else ""
    except Exception as e:
        print_log(f"テキスト取得中にエラーが発生しました - セレクタ: {selector}, エラー: {str(e)}")
        return ""

async def get_element_attribute(element: ElementHandle, selector: str, attribute: str) -> str:
    """要素から指定されたセレクタの属性値を取得する"""
    try:
        target_element = await element.query_selector(selector)
        return await target_element.get_attribute(attribute) if target_element else ""
    except Exception as e:
        print_log(f"属性取得中にエラーが発生しました - セレクタ: {selector}, 属性: {attribute}, エラー: {str(e)}")
        return ""

async def get_preview_url(row_element: ElementHandle) -> str:
    """求人のプレビューURLを取得する"""
    selector = 'td.data div.main > a[href^="https://en-gage.net/company/popup/job/"]'
    return await get_element_attribute(row_element, selector, 'href')

async def extract_applicant_info(modal: ElementHandle, page: Page) -> Dict[str, str]:
    """モーダルウィンドウから応募者情報を抽出する"""
    details = {}

    name_element = await modal.query_selector('div.account em')
    if name_element:
        details['名前'] = (await name_element.inner_text()).replace('\u3000', ' ')

    furigana_elements = await modal.query_selector_all('div.account em ruby rt')
    details['ふりがな'] = ' '.join([await elem.inner_text() for elem in furigana_elements])

    details = clean_name(details)
    details.update(await extract_additional_info(modal, page))

    return details

def clean_name(details: Dict[str, str]) -> Dict[str, str]:
    """名前からふりがなを削除する"""
    if '名前' in details and 'ふりがな' in details:
        name_parts = details['名前'].split()
        furigana_parts = details['ふりがな'].split()
        
        cleaned_name_parts = []
        used_furigana = set()

        for part in name_parts:
            cleaned_part = part
            for furigana in furigana_parts:
                if furigana in cleaned_part and furigana not in used_furigana:
                    cleaned_part = cleaned_part.replace(furigana, '', 1)
                    used_furigana.add(furigana)
                    break
            if cleaned_part:
                cleaned_name_parts.append(cleaned_part)
        
        details['名前'] = ' '.join(cleaned_name_parts)
    
    return details
async def extract_additional_info(modal: ElementHandle, page: Page) -> Dict[str, str]:
    """モーダルウィンドウから追加情報を抽出し、「選考へ進める」ボタンをクリックする"""
    details = {}

    info_elements = await modal.query_selector_all('div.account span.set')
    for element in info_elements:
        text = await element.inner_text()
        if '歳' in text:
            details['年齢'] = text
        elif text in ['男性', '女性']:
            details['性別'] = text

    details['生年月日'] = (await get_element_text(modal, 'dt.item:has-text("年齢") + dd.data')).split('（')[0].strip()
    details['タイトル'] = await get_element_text(modal, 'dl.md_horizonTable.long dd.data.long a')
    details['現職'] = await get_element_text(modal, 'dt.item:has-text("就業経験") + dd.data')
    details['住所'] = (await get_element_text(modal, 'dt.item:has-text("現住所") + dd.data')).replace('\n', '').strip()
    details['学歴'] = (await get_element_text(modal, 'dt.item:has-text("最終学歴") + dd.data')).split('（')[0].strip().split('/')[0].strip()
    details['応募日時'] = await get_element_text(modal, 'dt.item:has-text("応募日") + dd.data')

    details['施設形態'] = get_matched_items(details['タイトル'], FACILITY_TYPES)
    details['職種'] = get_matched_items(details['タイトル'], PROFESSIONS)

    try:
        button_selector = 'a.md_btn.md_btn--matching.js_modalCommit.js_applicantProcessing[data-modal_action="applicantOk"]'
        button = await modal.wait_for_selector(button_selector, state='visible', timeout=3000)
        if button:
            await button.click()
            print_log("「選考へ進める」ボタンをクリックしました。")
            await human_delay(200, 400)
            # モーダルの閉じるボタンがあれば閉じる
            await close_modal_if_exists(page)
        else:
            print_log("「選考へ進める」ボタンが見つかりませんでした。")

    except Exception as e:
        print_log(f"「選考へ進める」ボタンのクリック中にエラーが発生しました: {str(e)}")

    return details

def get_matched_items(title: str, items: List[str]) -> str:
    """タイトルと一致する項目を取得する"""
    matched = [item for item in items if item in title]
    return ', '.join(matched) if matched else "情報なし"

async def get_job_location(page: Page) -> Dict[str, str]:
    """求人画面から勤務地情報を取得する"""
    location_element = await page.query_selector('dl.dataSet:has(h3.item.item--area:text("勤務地"))')
    if not location_element:
        return {"都道府県": "情報なし", "エリア": "情報なし"}

    location_data = await location_element.query_selector('dd.data span.explain.be_strong')
    if not location_data:
        return {"都道府県": "情報なし", "エリア": "情報なし"}

    location_text = await location_data.inner_text()
    prefecture_match = re.search(r'(.+?[都道府県])', location_text)
    
    if prefecture_match:
        prefecture = prefecture_match.group(1)
        return {
            "都道府県": prefecture,
            "エリア": AREA_MAPPING.get(prefecture, "不明")
        }
    else:
        return {"都道府県": "不明", "エリア": "不明"}
async def get_applicant_details(page: Page, modal: ElementHandle, row_element: ElementHandle, client_name: str, mail: str, context=None) -> Dict[str, str]:
    """応募者の詳細情報を取得する"""
    details = {}

    print_log("応募者詳細取得開始")
    preview_url = await get_preview_url(row_element)
    details['求人プレビューURL'] = preview_url

    parsed_url = urlparse(preview_url)
    query_params = parse_qs(parsed_url.query)

    details['work_id'] = query_params.get('work_id', [None])[0]
    details['応募者ID'] = query_params.get('apply_id', [None])[0]
    details['アカウントID'] = query_params.get('PK', [None])[0]
    details['ID'] = f"{details['work_id']}_{details['応募者ID']}"
    details['求人URL'] = f"https://en-gage.net/user/search/desc/{details['work_id']}/#/"

    print_log("モーダルから応募者情報を抽出中...")
    details.update(await extract_applicant_info(modal, page))
    print_log(f"応募者情報抽出完了: {details.get('名前', '不明')}")

    print_log("求人ページから勤務地情報を取得中...")
    job_page = await context.new_page() if context else await page.context.new_page()
    try:
        await job_page.goto(details['求人URL'], wait_until='domcontentloaded', timeout=15000)
        details.update(await get_job_location(job_page))
    except Exception as e:
        print_log(f"求人ページ取得エラー: {str(e)}")
        details.update({"都道府県": "情報なし", "エリア": "情報なし"})
    finally:
        await job_page.close()
    print_log("勤務地情報取得完了")

    print_log("選考中ページから連絡先情報を取得中...")
    applicant_info = await get_applicant_info(page, details['応募者ID'])
    if applicant_info:
        details.update(applicant_info)
        print_log(f"連絡先取得完了: 電話={applicant_info.get('電話番号', 'なし')}, メール={applicant_info.get('メールアドレス', 'なし')}")
    else:
        print_log("連絡先情報が取得できませんでした")

    details['クライアント'] = client_name
    details['施設形態詳細'] = "情報なし"
    details['アカウントID'] = mail

    print_log("応募者詳細取得完了")
    return details

async def login_to_website(page: Page, user: User) -> bool:
    """ウェブサイトにログインする（自動ログイン方式）"""
    LOGIN_URL = "https://en-gage.net/company_login/login/"
    MANAGE_URL = "https://en-gage.net/company/manage/"

    # 1. ログインページに遷移
    await page.goto(LOGIN_URL, wait_until='domcontentloaded')
    print_log(f'{user.client_name} のログイン処理を開始...')
    await human_delay(800, 1500)

    # 2. 前回のセッションが残っている場合（ログインページではなく管理画面等にリダイレクトされた）
    if '/company_login/login' not in page.url:
        print_log(f'前回のセッションが残っています（{page.url}）。ログアウトします...')
        await page.goto('https://en-gage.net/company_login/auth/logout/', wait_until='domcontentloaded')
        await human_delay(500, 1000)
        # ログアウト後、ログインページに遷移
        if '/company_login/login' not in page.url:
            await page.goto(LOGIN_URL, wait_until='domcontentloaded')
            await human_delay(800, 1500)

    # 3. ID・パスワードを自動入力
    await page.fill('input[name="loginID"]', user.user_id)
    await human_delay(300, 600)
    await page.fill('input[name="password"]', user.password)
    await human_delay(500, 1000)

    # 4. ログインボタンをクリックしてページ遷移を待機
    try:
        async with page.expect_navigation(wait_until='domcontentloaded', timeout=30000):
            await page.click('#login-button')
        print_log(f'{user.client_name} のログインボタンをクリックしました')
        await human_delay(1000, 2000)
    except PlaywrightTimeoutError:
        print_log("ログインボタンクリック後のページ遷移タイムアウト")
        return False

    # 5. ログイン後もまだログインページにいる場合 → reCAPTCHAが出ている可能性
    if '/company_login/login' in page.url:
        print_log("ログイン後もログインページのまま。reCAPTCHAを確認...")
        recaptcha_solved = await handle_recaptcha_if_present(page)
        if recaptcha_solved:
            # ID/パスワードが消えている場合は再入力
            login_id_value = await page.input_value('input[name="loginID"]')
            if not login_id_value:
                await page.fill('input[name="loginID"]', user.user_id)
                await human_delay(300, 600)
                await page.fill('input[name="password"]', user.password)
                await human_delay(500, 1000)

            # フォーム直接送信（JSバリデーションをバイパス）
            print_log("reCAPTCHA解決後、フォームを直接送信します...")
            try:
                # form.submit()はページ遷移を起こすのでevaluateがエラーになる場合がある
                await page.evaluate('() => { const f = document.querySelector("form"); if (f) f.submit(); }')
            except Exception:
                pass  # ナビゲーションでコンテキスト破壊は想定内
            # ページ遷移を待機
            try:
                await page.wait_for_load_state('domcontentloaded', timeout=15000)
            except PlaywrightTimeoutError:
                pass
            await human_delay(1000, 2000)

            # まだログインページの場合、ボタンクリックも試す
            if '/company_login/login' in page.url:
                print_log("フォーム送信で遷移せず。ボタンクリックを試みます...")
                try:
                    async with page.expect_navigation(wait_until='domcontentloaded', timeout=15000):
                        await page.click('#login-button')
                    print_log("ボタンクリックでログイン成功")
                except PlaywrightTimeoutError:
                    print_log("ボタンクリックでもページ遷移しませんでした")

            if '/company_login/login' in page.url:
                print_log("reCAPTCHA解決後のログイン再送信に失敗")
                return False

            print_log("reCAPTCHA解決後のログイン成功")
        else:
            print_log("reCAPTCHAなし、またはreCAPTCHA解決失敗。ログイン失敗。")
            return False

    # 6. 管理画面に遷移（まだの場合）
    if '/company/manage/' not in page.url and '/company_login/login' not in page.url:
        await page.goto(MANAGE_URL, wait_until='domcontentloaded')
    elif '/company_login/login' in page.url:
        print_log("ログイン失敗（reCAPTCHA未解決またはID/パスワード不正）")
        return False
    print_log(f'管理画面に遷移完了: {page.url}')

    # モーダルを閉じる
    await close_modal_if_exists(page)

    # ポップアップ等を閉じるためのクリック
    await page.click('body', position={'x': 0, 'y': 0})
    await human_delay(200, 300)
    await close_modal_if_exists(page)

    return True

async def process_single_row(page: Page, row_element: ElementHandle, client_name: str, mail: str, context=None) -> Optional[Dict[str, str]]:
    """1行のデータを処理する"""
    # 処理開始前にモーダルを閉じる
    await close_modal_if_exists(page)

    profile_button = await row_element.query_selector('a.md_btn.md_btn--matchingDetail.js_modalOpenEx')
    if not profile_button:
        print_log("プロフィール確認ボタンが見つかりません")
        return None

    # プロフィールボタンクリック（モーダル遮断時はforce=Trueでリトライ）
    try:
        await profile_button.click(timeout=10000)
    except PlaywrightTimeoutError:
        print_log("プロフィールボタンクリックがモーダルに遮断されました。強制クリックを試みます...")
        await close_modal_if_exists(page)
        try:
            await profile_button.click(force=True, timeout=10000)
        except Exception as e:
            print_log(f"強制クリックも失敗: {str(e)}")
            await close_modal_if_exists(page)
            return None

    modal = await wait_for_element(page, '.base#js_applicantDetail')
    if not modal:
        print_log("モーダルウィンドウが表示されませんでした")
        await close_modal_if_exists(page)
        return None

    result = await get_applicant_details(page, modal, row_element, client_name, mail, context)
    await close_modal_if_exists(page)
    return result
async def get_applicant_info(page, target_apply_id):
    try:
        await page.goto("https://en-gage.net/company/manage/processing/", wait_until='domcontentloaded', timeout=30000)
    except PlaywrightTimeoutError:
        print_log("選考中ページへの遷移タイムアウト（続行します）")
    await human_delay(300, 500)

    details = {}
    for seq in range(1, 101):  # max_attempts = 100
        row = await page.query_selector(f'tr[data-seq="{seq}"]')
        if not row:
            break

        link = await row.query_selector('td.data a[href^="https://en-gage.net/company/popup/job/"]')
        if not link:
            continue

        href = await link.get_attribute('href')
        apply_id_match = re.search(r'apply_id=([^&]+)', href)
        if not apply_id_match or apply_id_match.group(1) != target_apply_id:
            continue

        profile_button = await row.query_selector('a.js_drawerProfileOpen')
        if not profile_button:
            continue

        await profile_button.click()
        modal = await wait_for_element(page, '.base#js_showApplyData', timeout=10000)
        if not modal:
            continue

        await page.wait_for_selector('.tabContent--profile', state='visible', timeout=10000)

        selectors = [
            ('.md_list--data li.row:has(.label:text-is("電話番号")) .data', '電話番号'),
            ('.md_list--data li.row:has(.label:text-is("メールアドレス")) .data', 'メールアドレス'),
            ('.md_list--data li.row:has(.label:text-is("現住所")) .data', '_住所_raw'),
        ]

        for selector, key in selectors:
            element = await modal.query_selector(selector)
            if element:
                details[key] = (await element.inner_text()).strip()

        # 郵便番号を抽出し XXX-XXXX 形式に統一（〒6408301 / 〒640-8301 両対応）
        raw_address = details.pop('_住所_raw', '')
        if raw_address:
            postal_match = re.search(r'〒\s*(\d{3})-?(\d{4})', raw_address)
            if postal_match:
                details['郵便番号'] = f"{postal_match.group(1)}-{postal_match.group(2)}"

        close_button = await modal.query_selector('.md_modal__close')
        if close_button:
            await close_button.click()
        # 追加の閉じるボタンもチェック
        await close_modal_if_exists(page)

        return details

    return details

async def run_scraper(playwright) -> List[Dict[str, str]]:
    """スクレイピングを実行する"""
    notification_manager = NotificationManager(config)

    active_accounts = get_active_accounts()
    if not active_accounts:
        print_log("有効なアカウントが見つかりませんでした。")
        return []

    all_data = []
    context = None
    page = None

    # Bot専用のプロファイルディレクトリ（Cookie等を永続化）
    profile_dir = os.path.join(CURRENT_DIR, "chrome_profile")

    # 前回クラッシュ時の残存Chromeプロセスを強制終了し、ロックファイルをクリーンアップ
    import subprocess
    try:
        subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'], capture_output=True, timeout=10)
        print_log("残存Chromeプロセスを終了しました")
        time.sleep(2)  # プロセス終了を待つ
    except Exception as e:
        print_log(f"Chromeプロセス終了処理: {e}")

    for lock_file in ["SingletonLock", "SingletonSocket", "SingletonCookie", "lockfile"]:
        lock_path = os.path.join(profile_dir, lock_file)
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
                print_log(f"ロックファイルを削除: {lock_file}")
            except Exception as e:
                print_log(f"ロックファイル削除失敗: {lock_file} ({e})")

    for account in active_accounts:
        # 実行履歴の記録を開始
        client_start = time.time()
        record = ExecutionRecord(
            start_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            client_name=account.client_name,
        )
        written_count = 0
        duplicate_count = 0
        new_applicants = 0

        # ブラウザが未起動または再起動が必要な場合
        if context is None:
            print_log("ブラウザを起動中...")
            context = await playwright.chromium.launch_persistent_context(
                profile_dir,
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
            page = context.pages[0] if context.pages else await context.new_page()
            stealth = Stealth()
            await stealth.apply_stealth_async(page)

        if not await login_to_website(page, account):
            print_log(f"{account.client_name}のログインに失敗しました。次のアカウントに進みます。")
            record.login_result = '失敗'
            record.recaptcha_status = _last_recaptcha_status
            record.captcha_solve_time = _last_captcha_solve_time
            record.processing_time = f"{time.time() - client_start:.1f}"
            write_execution_log(record)
            continue

        record.login_result = '成功'
        record.recaptcha_status = _last_recaptcha_status
        record.captcha_solve_time = _last_captcha_solve_time

        print_log(f"{account.client_name}のログインに成功しました。")
        await human_delay(1000, 2000)

        consecutive_failures = 0
        MAX_CONSECUTIVE_FAILURES = 3

        while True:
            # 「新着の応募はありません」チェック
            no_data_element = await page.query_selector('#js_applicantNoData')
            if no_data_element:
                is_visible = await no_data_element.is_visible()
                if is_visible:
                    print_log(f"{account.client_name}: 新着の応募はありません。次のアカウントへ進みます。")
                    break

            row_selector = 'tbody#js_applicantList tr[data-seq="1"]'
            row_element = await wait_for_element(page, row_selector, timeout=5000)

            if not row_element:
                print_log(f"{account.client_name}のデータ取得完了。新規データがありません。")
                break

            print_log(f"{account.client_name}の新規データを処理中")
            new_applicants += 1

            try:
                data = await process_single_row(page, row_element, account.client_name, account.user_id, context)
                if data:
                    consecutive_failures = 0
                    all_data.append(data)

                    email = data.get('メールアドレス', '')
                    job_url = data.get('求人URL', '')

                    # 重複チェック: 同一メールアドレス AND 同一求人URLの場合は弾く
                    if not SpreadsheetManager.check_duplicate_application(email, job_url):
                        # 通知送信（エラーでも処理を継続）
                        try:
                            await send_notification(notification_manager, account.user_id, data)
                        except Exception as notify_err:
                            print_log(f"通知送信エラー（処理は継続）: {type(notify_err).__name__}: {str(notify_err)}")
                        # スプレッドシートに書き込み
                        write_to_spreadsheet(data)
                        written_count += 1
                    else:
                        print_log(f"重複応募のため、スキップされました: {email} / {job_url}")
                        duplicate_count += 1
                else:
                    consecutive_failures += 1
                    print_log(f"{account.client_name}のデータはスキップされました（連続失敗: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}）")
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        print_log(f"{account.client_name}: 連続{MAX_CONSECUTIVE_FAILURES}回失敗のため、次のアカウントへ進みます")
                        record.error_message = f"連続{MAX_CONSECUTIVE_FAILURES}回データ取得失敗"
                        break
            except PlaywrightTimeoutError as e:
                consecutive_failures += 1
                print_log(f"{account.client_name}のデータ処理中にタイムアウトが発生しました（連続失敗: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}）: {str(e)}")
                record.error_message = f"タイムアウト: {str(e)}"
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print_log(f"{account.client_name}: 連続{MAX_CONSECUTIVE_FAILURES}回失敗のため、次のアカウントへ進みます")
                    break
            except Exception as e:
                consecutive_failures += 1
                import traceback
                print_log(f"{account.client_name}のデータ処理中にエラーが発生しました（連続失敗: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}）: {type(e).__name__}: {str(e)}")
                traceback.print_exc()
                record.error_message = f"{type(e).__name__}: {str(e)}"
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print_log(f"{account.client_name}: 連続{MAX_CONSECUTIVE_FAILURES}回失敗のため、次のアカウントへ進みます")
                    break

            try:
                await page.goto("https://en-gage.net/company/manage/", wait_until='domcontentloaded', timeout=30000)
            except PlaywrightTimeoutError:
                print_log("ページ遷移タイムアウト（続行します）")
            await human_delay(1000, 2000)

        # ログアウト処理
        logout_success = False
        try:
            await logout(page)
            logout_success = True
        except PlaywrightTimeoutError:
            print_log(f"{account.client_name}のログアウトがタイムアウトしました")
        except Exception as e:
            print_log(f"{account.client_name}のログアウト中にエラー: {str(e)}")

        # ログアウト失敗時はブラウザを再起動
        if not logout_success:
            print_log("ブラウザを再起動します...")
            try:
                await context.close()
            except Exception:
                pass
            context = None
            page = None

        # 実行履歴を記録
        record.new_applicants = new_applicants
        record.written_count = written_count
        record.duplicate_count = duplicate_count
        record.processing_time = f"{time.time() - client_start:.1f}"
        write_execution_log(record)

        print_log(f"{account.client_name}の対応が完了しました。")

    print_log("全アカウントの処理が完了しました。")
    if context:
        await context.close()
    return all_data

async def send_notification(notification_manager: NotificationManager, account_email: str, data: Dict[str, str]):
    """通知を送信する"""
    message = f'''🎉：5分以内に新規応募
・アカウント：{account_email}
・職種：{data.get('職種', '情報なし')}
・名前：{data.get('名前', '情報なし')}
・年齢：{data.get('年齢', '情報なし')}
・住所：{data.get('住所', '情報なし')}
・電話：{data.get('電話番号', '情報なし')}
・タイトル：{data.get('タイトル', '情報なし')}
・求人：{data.get('求人URL', '情報なし')}'''
    await notification_manager.send_notifications_async(message)
    print_log("通知を送信しました。")

async def logout(page: Page):
    """ログアウトする（ログアウトURLに直接遷移）"""
    await page.goto('https://en-gage.net/company_login/auth/logout/', wait_until='domcontentloaded')
    await human_delay(800, 1500)
    print_log(f'ログアウト完了（{page.url}）')

async def main() -> List[Dict[str, str]]:
    """メイン関数"""
    async with async_playwright() as playwright:
        scraper_data_list = await run_scraper(playwright)
    return scraper_data_list

if __name__ == "__main__":
    scraper_data_list = asyncio.run(main())
