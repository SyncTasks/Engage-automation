"""
Engage応募通知 → Chatwork/LINE連携

- ムームードメインのメールサーバーにIMAP直接接続
- 「ユーザ」シートから対象アカウント取得（媒体名=engage, is_active=TRUE）
- 「通知設定」シートから通知先設定を取得（通知設定名でマッチ）
- 職種・施設形態マッピングを外部スプレッドシートから取得
- ChatGPT連携で都道府県・企業名抽出
- 並列処理対応（最大2スレッド）
- 重複防止: IMAPフラグ（処理済みメールにスターを付与）

環境変数:
  - GOOGLE_CREDENTIALS: Google サービスアカウントのJSON
  - OPENAI_API_KEY: OpenAI APIキー（都道府県・企業名抽出用）
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
import time


class TeeWriter:
    """stdoutとログファイルの両方に書き込む"""
    def __init__(self, log_path):
        self._stdout = sys.stdout
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        self._file = open(log_path, 'a', encoding='utf-8')

    def write(self, text):
        self._stdout.write(text)
        self._stdout.flush()
        self._file.write(text)
        self._file.flush()

    def flush(self):
        self._stdout.flush()
        self._file.flush()

    def close(self):
        self._file.close()

# 日本時間 (JST = UTC+9)
JST = timezone(timedelta(hours=9))

from dotenv import load_dotenv
import gspread

# .envファイルを読み込み（スクリプトと同じフォルダの.envを使用）
_script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_script_dir, '.env'))
import requests
from google.oauth2.service_account import Credentials
import dns.resolver
from imap_tools import MailBox, AND, OR


# ===== 設定 =====
ENGAGE_SENDER_ADDRESSES = ['system@en-gage.net', 'noreply@en-gage.net']  # Engageからの通知メール送信元

# ドメイン → IMAPサーバーの既知マッピング
KNOWN_IMAP_HOSTS = {
    'muumuu-mail.com': 'imap4.muumuu-mail.com',
    'lolipop.jp': 'imap4.lolipop.jp',
    'xserver.jp': 'sv*.xserver.jp',
    'sakura.ne.jp': 'www*.sakura.ne.jp',
    'yahoo.co.jp': 'imap.mail.yahoo.co.jp',
    'outlook.jp': 'outlook.office365.com',
    'gmail.com': 'imap.gmail.com',
    'googlemail.com': 'imap.gmail.com',
}


def resolve_imap_server(email):
    """メールアドレスのドメインからIMAPサーバーを自動判定"""
    domain = email.split('@')[-1].lower()

    # 1. 既知ドメインの直接マッピング
    if domain in KNOWN_IMAP_HOSTS:
        host = KNOWN_IMAP_HOSTS[domain]
        print(f'  [IMAP判定] {domain} → {host} (既知ドメイン)')
        return host

    # 2. MXレコードを検索
    try:
        mx_records = dns.resolver.resolve(domain, 'MX')
        mx_hosts = [str(r.exchange).rstrip('.').lower() for r in mx_records]
        print(f'  [IMAP判定] {domain} MXレコード: {mx_hosts}')
    except Exception as e:
        print(f'  [IMAP判定] {domain} MXレコード取得失敗: {e}')
        fallback = f'imap.{domain}'
        print(f'  [IMAP判定] フォールバック: {fallback}')
        return fallback

    # 3. MXホストからプロバイダを推測
    for mx_host in mx_hosts:
        if 'google.com' in mx_host or 'googlemail.com' in mx_host:
            print(f'  [IMAP判定] {domain} → imap.gmail.com (Google Workspace)')
            return 'imap.gmail.com'
        if 'amazonaws.com' in mx_host:
            print(f'  [IMAP判定] {domain} → AWS SES（非対応）')
            return 'AWS SES（非対応）'
        if 'outlook.com' in mx_host or 'protection.outlook.com' in mx_host:
            print(f'  [IMAP判定] {domain} → outlook.office365.com (Microsoft 365)')
            return 'outlook.office365.com'
        if 'muumuu-mail.com' in mx_host or 'lolipop.jp' in mx_host:
            host = 'imap4.muumuu-mail.com'
            print(f'  [IMAP判定] {domain} → {host} (MX: {mx_host})')
            return host

    # 4. MXホスト名をそのまま使用
    mx_primary = mx_hosts[0] if mx_hosts else None
    if mx_primary:
        print(f'  [IMAP判定] {domain} → {mx_primary} (MXホスト名をそのまま使用)')
        return mx_primary

    fallback = f'imap.{domain}'
    print(f'  [IMAP判定] {domain} → {fallback} (最終フォールバック)')
    return fallback

# スプレッドシートID（設定情報の読み取り用）
CONFIG_SPREADSHEET_ID = '1HzSM76jUtUOzHiy1zg3Ivqg_-nTn0iFwVwrzG82hQzU'  # ログイン情報・通知設定
MAPPING_SPREADSHEET_ID = '13SErWeTXqTbqgR3n16GT1__8LLI2r8egGx4defJbQ9k'  # マッピングシート

# シート名
CONFIG_SHEET_NAME = 'ユーザ'
NOTIFY_SHEET_NAME = '通知設定'
JOB_MAPPING_SHEET_NAME = '職種_Akindo独自'
FACILITY_MAPPING_SHEET_NAME = '施設形態_Akindo独自'

# Chatwork通知先（全アカウント共通）
CHATWORK_ROOM_ID = os.environ.get('CHATWORK_ROOM_ID', '')
CHATWORK_TOKEN = os.environ.get('CHATWORK_TOKEN', '')

# 即時反応用LINE通知設定
INSTANT_LINE_ACCESS_TOKEN = "Mw2VoARIaLmAZw39NlvSlwRYIWcSMZ4A449jrgMK/5aBmipl29RcTz8WI8u964aukUTtMj+MXiOE8iTp2JP2ExWILa2rRZYC2GtgNxbejkXeuiF/9P5hTNvIRoQyNJAUY+7UZSK0kJvQZqrK0qqSugdB04t89/1O/w1cDnyilFU="
INSTANT_LINE_GROUP_ID = "C1735273ce570b8b4817c1734ff5acec6"

# 検索対象期間（日数）
SEARCH_DAYS_AGO = 1  # 1日以内のメールのみ処理

# 都道府県リスト
PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
]

# 都道府県→エリアマッピング
REGION_MAPPINGS = {
    "北海道": "北海道",
    "青森県": "東北", "岩手県": "東北", "宮城県": "東北", "秋田県": "東北", "山形県": "東北", "福島県": "東北",
    "茨城県": "関東", "栃木県": "関東", "群馬県": "関東", "埼玉県": "関東", "千葉県": "関東", "東京都": "関東", "神奈川県": "関東",
    "新潟県": "中部", "富山県": "中部", "石川県": "中部", "福井県": "中部", "山梨県": "中部", "長野県": "中部", "岐阜県": "中部", "静岡県": "中部", "愛知県": "中部",
    "三重県": "関西", "滋賀県": "関西", "京都府": "関西", "大阪府": "関西", "兵庫県": "関西", "奈良県": "関西", "和歌山県": "関西",
    "鳥取県": "中国", "島根県": "中国", "岡山県": "中国", "広島県": "中国", "山口県": "中国",
    "徳島県": "四国", "香川県": "四国", "愛媛県": "四国", "高知県": "四国",
    "福岡県": "九州", "佐賀県": "九州", "長崎県": "九州", "熊本県": "九州", "大分県": "九州", "宮崎県": "九州", "鹿児島県": "九州", "沖縄県": "九州"
}


def get_env_optional(key: str) -> str:
    """環境変数を取得（オプション）"""
    return os.environ.get(key, '')


# ===== 情報抽出関数 =====

def extract_job_title(subject: str) -> str:
    """件名から求人タイトルを抽出（Engage形式）

    Engageの件名パターン:
    - 「【要対応】新着応募のお知らせ【職種名：画像データのチェック事務スタッフ】」
    """
    match = re.search(r'【職種名[：:](.+?)】', subject)
    if match:
        return match.group(1).strip()
    return ''


def extract_apply_id(body_text: str) -> str:
    """メール本文から応募IDを抽出

    URL例: https://en-gage.net/company/manage/message/?apply_id=MTg2NTg1NzQ=
    """
    match = re.search(r'apply_id=([A-Za-z0-9+/=]+)', body_text)
    if match:
        return match.group(1).strip()
    return ''


def extract_location_from_body(body_text: str) -> str:
    """メール本文から勤務地を抽出"""
    if not body_text:
        return ''

    text = re.sub(r'<[^>]+>', ' ', body_text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'\s+', ' ', text)

    prefectures_pattern = '|'.join(re.escape(p) for p in sorted(PREFECTURES, key=len, reverse=True))

    pattern = rf'勤務地[：:\s]*((?:{prefectures_pattern})[^\s]*)'
    match = re.search(pattern, text)
    if match:
        return match.group(1).strip()

    return ''


def determine_job_types(title: str, job_mappings: Dict[str, List[str]]) -> List[str]:
    """職種を判定"""
    job_types = []
    for job_type, keywords in job_mappings.items():
        for keyword in keywords:
            if keyword and keyword in title:
                job_types.append(job_type)
                break
    return list(set(job_types))


def determine_facility_type(title: str, facility_mappings: Dict[str, List[str]]) -> tuple:
    """施設形態を判定"""
    for facility_type, keywords in facility_mappings.items():
        for keyword in keywords:
            if keyword and keyword in title:
                return facility_type, keyword
    return '', ''


def extract_prefecture_from_body(body_text: str) -> Optional[str]:
    """メール本文から都道府県を抽出"""
    if not body_text:
        return None

    text = re.sub(r'<[^>]+>', ' ', body_text)
    text = re.sub(r'&nbsp;', ' ', text)

    for pref in sorted(PREFECTURES, key=len, reverse=True):
        pattern = rf'勤務地[^\n]{{0,30}}{re.escape(pref)}'
        if re.search(pattern, text):
            return pref

    for pref in sorted(PREFECTURES, key=len, reverse=True):
        if pref in text:
            return pref

    return None


def extract_info_with_chatgpt(html_body: str) -> dict:
    """ChatGPTを使用して本文から都道府県・企業名を抽出"""
    api_key = get_env_optional('OPENAI_API_KEY')

    if not api_key:
        print("OPENAI_API_KEY未設定、ChatGPT抽出をスキップ")
        return {'prefecture': None, 'company_name': None}

    api_url = 'https://api.openai.com/v1/chat/completions'

    response_schema = {
        "type": "object",
        "properties": {
            "prefecture": {"type": "string", "description": "勤務地の都道府県名。見つからなければ null。例：東京都"},
            "companyName": {"type": "string", "description": "応募先の会社名や施設名。見つからなければ null。例：株式会社○○、○○病院"}
        },
        "required": ["prefecture", "companyName"],
        "additionalProperties": False
    }

    payload = {
        "model": "gpt-4.1-nano",
        "messages": [
            {
                "role": "system",
                "content": "あなたはHTMLからデータを抽出する専門家です。HTMLを解析して必要な情報を正確に抽出してください。"
            },
            {
                "role": "user",
                "content": f"以下のHTMLから、勤務地の都道府県名、応募先の会社名や施設名を抽出してください。\n\nHTML:\n{html_body[:10000]}"
            }
        ],
        "temperature": 0.1,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "ExtractJobInfo",
                "strict": True,
                "schema": response_schema
            }
        }
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    for attempt in range(3):
        try:
            response = requests.post(api_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()

            json_response = response.json()
            content = json_response['choices'][0]['message']['content']
            result = json.loads(content)

            print(f"ChatGPT抽出結果: {result}")
            return {
                'prefecture': result.get('prefecture'),
                'company_name': result.get('companyName')
            }
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = (attempt + 1) * 5
                print(f"ChatGPT APIレート制限、{wait}秒待機して再試行 ({attempt + 1}/3)")
                time.sleep(wait)
                continue
            print(f"ChatGPT APIエラー: {e}")
            return {'prefecture': None, 'company_name': None}
        except Exception as e:
            print(f"ChatGPT APIエラー: {e}")
            return {'prefecture': None, 'company_name': None}

    print("ChatGPT API: 3回リトライしても失敗")
    return {'prefecture': None, 'company_name': None}


def get_region(prefecture: str) -> str:
    """都道府県からエリアを取得"""
    return REGION_MAPPINGS.get(prefecture, '')


# ===== Google Sheets関連（読み取りのみ） =====

def get_sheets_client():
    """Google Sheets クライアントを取得"""
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly'
    ]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_file = os.path.join(script_dir, 'credentials.json')

    if os.path.exists(creds_file):
        try:
            creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
            print('認証: credentials.json を使用')
            return gspread.authorize(creds)
        except Exception as e:
            print(f'credentials.json 読み込みエラー: {e}')

    creds_json = get_env_optional('GOOGLE_CREDENTIALS')
    if creds_json:
        try:
            creds_dict = json.loads(creds_json)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            print('認証: GOOGLE_CREDENTIALS 環境変数を使用')
            return gspread.authorize(creds)
        except Exception as e:
            print(f'GOOGLE_CREDENTIALS 解析エラー: {e}')

    print('エラー: Google認証情報が見つかりません')
    return None


def get_notification_settings(client) -> Dict[str, dict]:
    """通知設定シートから設定を取得"""
    if not client:
        return {}

    try:
        spreadsheet = client.open_by_key(CONFIG_SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(NOTIFY_SHEET_NAME)
        records = worksheet.get_all_records()

        settings = {}
        for record in records:
            name = record.get('通知設定名', '')
            if name:
                settings[name] = {
                    'is_test': record.get('is_test', False),
                    'chatwork_notify_enabled': record.get('chatwork_notify_enabled', False),
                    'chatwork_api_token': record.get('chatwork_api_token', ''),
                    'chatwork_room_id': str(record.get('chatwork_room_id', '')),
                    'chatwork_test_room_id': str(record.get('chatwork_test_room_id', '')),
                    'line_notify_enabled': record.get('line_notify_enabled', False),
                    'line_notify_access_token': record.get('line_notify_access_token', ''),
                    'line_test_notify_access_token': record.get('line_test_notify_access_token', ''),
                }

        print(f"通知設定を取得: {len(settings)}件")
        return settings

    except Exception as e:
        print(f'通知設定取得エラー: {e}')
        return {}


def get_login_credentials(client, notification_settings: Dict[str, dict], instant_only: bool = False) -> List[dict]:
    """ログイン情報シートから認証情報を取得（engage + is_active=TRUE）

    IMAP列が空の場合はメールアドレスのドメインから自動判定する。
    """
    if not client:
        return []

    try:
        spreadsheet = client.open_by_key(CONFIG_SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(CONFIG_SHEET_NAME)
        records = worksheet.get_all_records()

        credentials = []
        for i, record in enumerate(records):
            email = record.get('メール', '')
            password = record.get('パス', '')
            client_name = record.get('クライアント名', '')
            media = record.get('媒体名', '')
            is_active = record.get('is_active', False)
            notify_setting_name = record.get('通知設定名', '')
            imap_server = str(record.get('IMAP', '')).strip()
            imap_password = str(record.get('IMAPパス', '')).strip()

            is_active_bool = is_active == True or str(is_active).upper() == 'TRUE'
            if not (email and password and media == 'engage' and is_active_bool):
                continue

            instant_flag = record.get('即時反応', False)
            is_instant = instant_flag == True or str(instant_flag).upper() == 'TRUE'

            if instant_only and not is_instant:
                continue
            if not instant_only and is_instant:
                continue

            # IMAP列が空の場合はメールアドレスから自動判定
            if not imap_server:
                imap_server = resolve_imap_server(email)
                print(f'  [{client_name}] IMAP自動判定: {imap_server}')
            else:
                print(f'  [{client_name}] IMAP: {imap_server} (スプレッドシート設定)')

            notify_config = notification_settings.get(notify_setting_name, {})

            credentials.append({
                'email': email,
                'password': password,
                'imap_server': imap_server,
                'imap_password': imap_password or password,
                'client_name': client_name,
                'notify_setting_name': notify_setting_name,
                'notify_config': notify_config
            })

        mode_label = "即時反応" if instant_only else "通常"
        print(f"取得したログイン情報: {len(credentials)}件 ({mode_label}モード)")
        return credentials

    except Exception as e:
        print(f'ログイン情報取得エラー: {e}')
        return []


def get_job_mappings(client) -> Dict[str, List[str]]:
    """職種マッピングをスプレッドシートから取得"""
    if not client:
        return {}

    try:
        spreadsheet = client.open_by_key(MAPPING_SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(JOB_MAPPING_SHEET_NAME)
        records = worksheet.get_all_records()

        mappings = {}
        for record in records:
            category = record.get('職業カテゴリー', '')
            keywords = record.get('判別ワード', '')

            if category and keywords:
                if category not in mappings:
                    mappings[category] = []
                keywords_list = [k.strip() for k in keywords.split(',')]
                mappings[category].extend(keywords_list)

        print(f"職種マッピング取得: {len(mappings)}カテゴリ")
        return mappings

    except Exception as e:
        print(f'職種マッピング取得エラー: {e}')
        return {}


def get_facility_mappings(client) -> Dict[str, List[str]]:
    """施設形態マッピングをスプレッドシートから取得"""
    if not client:
        return {}

    try:
        spreadsheet = client.open_by_key(MAPPING_SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(FACILITY_MAPPING_SHEET_NAME)
        records = worksheet.get_all_records()

        mappings = {}
        for record in records:
            category = record.get('施設カテゴリー', '')
            keywords = record.get('判別ワード', '')

            if category and keywords:
                if category not in mappings:
                    mappings[category] = []
                keywords_list = [k.strip() for k in keywords.split(',')]
                mappings[category].extend(keywords_list)

        print(f"施設形態マッピング取得: {len(mappings)}カテゴリ")
        return mappings

    except Exception as e:
        print(f'施設形態マッピング取得エラー: {e}')
        return {}


# ===== 通知関連 =====

def send_to_chatwork(token: str, room_id: str, message: str) -> bool:
    """Chatworkにメッセージを送信"""
    if not token or not room_id:
        print("Chatwork設定が不完全です")
        return False

    url = f'https://api.chatwork.com/v2/rooms/{room_id}/messages'
    headers = {'X-ChatWorkToken': token}
    data = {'body': message}

    try:
        response = requests.post(url, headers=headers, data=data, timeout=10)
        if response.status_code == 200:
            print(f"Chatwork通知成功 (room: {room_id})")
            return True
        else:
            print(f'Chatwork送信エラー: {response.status_code} {response.text}')
            return False
    except Exception as e:
        print(f'Chatwork送信例外: {e}')
        return False


def send_to_line(access_token: str, group_id: str, message: str) -> bool:
    """LINE Messaging APIでグループにメッセージを送信"""
    if not access_token or not group_id:
        print("LINE設定が不完全です")
        return False

    url = 'https://api.line.me/v2/bot/message/push'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    payload = {
        'to': group_id,
        'messages': [{'type': 'text', 'text': message}]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code == 200:
            print(f"LINE通知成功 (group: {group_id[:10]}...)")
            return True
        else:
            print(f'LINE送信エラー: {response.status_code} {response.text}')
            return False
    except Exception as e:
        print(f'LINE送信例外: {e}')
        return False


def format_line_message(data: dict) -> str:
    """LINE用メッセージをフォーマット"""
    date_str = data['date'].strftime('%Y/%m/%d %H:%M') if data.get('date') else ''
    client = data.get('client', '')

    lines = []
    lines.append(f'🟪 {client}：Engage新規応募【即時通知】')
    lines.append(f'━━━━━━━━━━━━━━')
    lines.append(f'応募職種: {data.get("title", "")}')
    if data.get('job_type'):
        lines.append(f'職種: {data.get("job_type", "")}')
    if data.get('company_name'):
        lines.append(f'応募先企業名: {data.get("company_name", "")}')
    location = data.get('location') or data.get('prefecture', '')
    if location:
        lines.append(f'勤務地: {location}')
    lines.append(f'応募日時: {date_str}')
    return '\n'.join(lines)


def send_notification(data: dict, notify_config: dict, instant_mode: bool = False):
    """通知設定に基づいて通知を送信"""
    if not instant_mode and not notify_config:
        print("通知設定がありません、通知をスキップ")
        return

    # Chatwork通知（一時的に無効化）
    # if CHATWORK_TOKEN and CHATWORK_ROOM_ID:
    #     message = format_chatwork_message(data, instant_mode=instant_mode)
    #     send_to_chatwork(CHATWORK_TOKEN, CHATWORK_ROOM_ID, message)

    # 即時反応モードの場合、LINEグループにも通知
    if instant_mode:
        line_message = format_line_message(data)
        send_to_line(INSTANT_LINE_ACCESS_TOKEN, INSTANT_LINE_GROUP_ID, line_message)


def format_chatwork_message(data: dict, instant_mode: bool = False) -> str:
    """Chatwork用メッセージをフォーマット"""
    date_str = data['date'].strftime('%Y/%m/%d %H:%M') if data.get('date') else ''

    client = data.get("client", "")
    instant_label = "【即時通知】" if instant_mode else ""
    lines = [f'[info][title]{client}：🎉 Engage新規応募{instant_label}[/title]']
    lines.append(f'・応募日時：{date_str}')
    lines.append(f'・応募職種：{data.get("title", "")}')
    if data.get('job_type'):
        lines.append(f'・職種：{data.get("job_type", "")}')
    if data.get('company_name'):
        lines.append(f'・応募先企業名：{data.get("company_name", "")}')
    location = data.get('location') or data.get('prefecture', '')
    if location:
        lines.append(f'・勤務地：{location}')
    if data.get('apply_url'):
        lines.append(f'・確認URL：{data.get("apply_url", "")}')
    lines.append('[/info]')

    return '\n'.join(lines)


# ===== メール処理 =====

def process_mailbox(credential: dict, sheets_client, job_mappings: dict, facility_mappings: dict, instant_mode: bool = False) -> int:
    """1つのメールアカウントを処理"""
    import traceback

    client_name = credential['client_name']
    email_user = credential['email']
    email_pass = credential['password']
    notify_config = credential.get('notify_config', {})
    notify_setting_name = credential.get('notify_setting_name', '')

    start_time = time.time()

    print(f'\n{"="*50}')
    print(f'処理開始: {client_name} ({email_user})')
    if not instant_mode:
        print(f'通知設定: {notify_setting_name}')
    print(f'開始時刻: {datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")}')
    print(f'{"="*50}')

    count = 0
    try:
        imap_server = credential.get('imap_server', '')
        imap_password = credential.get('imap_password', email_pass)
        if not imap_server:
            print(f'  [{client_name}] IMAPサーバー未設定、スキップ')
            return 0
        print(f'  [{client_name}] IMAP接続開始: {imap_server}...')
        mailbox = MailBox(imap_server, timeout=60)
        print(f'  [{client_name}] IMAP接続成功、ログイン中...')
        mailbox_ctx = mailbox.login(email_user, imap_password)
        print(f'  [{client_name}] ログイン成功')

        with mailbox_ctx as mb:
            # Engageからのフラグなしメールを検索（処理済みはフラグ付きなのでスキップされる）
            from_query = OR(AND(from_=ENGAGE_SENDER_ADDRESSES[0]), AND(from_=ENGAGE_SENDER_ADDRESSES[1]))
            query = AND(AND(flagged=False), from_query)
            print(f'  [{client_name}] メール検索中 (flagged=False, from={ENGAGE_SENDER_ADDRESSES})...')

            messages = list(mb.fetch(query))
            print(f'  [{client_name}] 取得メール数: {len(messages)}件')

            skipped_non_apply = 0
            skipped_old = 0
            skipped_no_title = 0

            for i, msg in enumerate(messages):
                from_addr = msg.from_ or ''
                print(f'  [{client_name}] メール #{i+1}/{len(messages)}: {msg.subject}')
                print(f'    送信元: {from_addr}')
                print(f'    日時: {msg.date}')

                # 件名に「応募」が含まれるか確認
                if '応募' not in msg.subject:
                    skipped_non_apply += 1
                    print(f'    → スキップ: 応募メールではない')
                    continue

                # メール日時をJSTに変換
                mail_date = msg.date
                if mail_date.tzinfo is not None:
                    mail_date_jst = mail_date.astimezone(JST)
                else:
                    mail_date_jst = mail_date.replace(tzinfo=timezone.utc).astimezone(JST)

                # 7日以内のメールのみ処理
                now_jst = datetime.now(JST)
                days_ago = (now_jst - mail_date_jst).days
                if days_ago > SEARCH_DAYS_AGO:
                    mb.flag(msg.uid, ['\\Flagged'], True)
                    skipped_old += 1
                    print(f'    → スキップ: {days_ago}日前（フラグ付与）')
                    continue

                # 件名から職種名を抽出
                job_title = extract_job_title(msg.subject)
                print(f'    求人タイトル: {job_title or "(抽出失敗)"}')

                if not job_title:
                    skipped_no_title += 1
                    print(f'    → スキップ: 職種名抽出失敗')
                    continue

                # メール本文を取得
                body_text = msg.text or ''
                html_body = msg.html or ''
                if not body_text and html_body:
                    body_text = re.sub(r'<[^>]+>', ' ', html_body)
                    body_text = re.sub(r'&nbsp;', ' ', body_text)
                    body_text = re.sub(r'\s+', ' ', body_text).strip()

                print(f'    --- メール本文 ---')
                print(f'    {body_text[:3000]}')
                print(f'    --- メール本文ここまで ---')

                # 応募IDとURLを抽出
                apply_id = extract_apply_id(body_text) or extract_apply_id(html_body)
                apply_url = ''
                url_match = re.search(r'(https://en-gage\.net/company/manage/message/\?apply_id=[A-Za-z0-9+/=]+)', body_text)
                if not url_match:
                    url_match = re.search(r'(https://en-gage\.net/company/manage/message/\?apply_id=[A-Za-z0-9+/=]+)', html_body)
                if url_match:
                    apply_url = url_match.group(1)
                print(f'    応募ID: {apply_id or "(なし)"}')
                print(f'    応募URL: {apply_url or "(なし)"}')

                # 職種・施設形態を判定
                job_types = determine_job_types(job_title, job_mappings)
                facility_type, facility_type_detail = determine_facility_type(job_title, facility_mappings)
                print(f'    職種: {", ".join(job_types) if job_types else "(該当なし)"}')
                print(f'    施設形態: {facility_type or "(該当なし)"} {facility_type_detail}')

                # 本文から都道府県を抽出
                prefecture = extract_prefecture_from_body(body_text)
                if not prefecture and html_body:
                    prefecture = extract_prefecture_from_body(html_body)
                company_name = ''
                print(f'    本文都道府県抽出: {prefecture or "(見つからず)"}')

                # ChatGPTで追加情報抽出
                chatgpt_input = html_body if html_body else body_text
                if chatgpt_input:
                    print(f'    ChatGPT抽出開始...')
                    chatgpt_start = time.time()
                    chatgpt_result = extract_info_with_chatgpt(chatgpt_input)
                    chatgpt_elapsed = time.time() - chatgpt_start
                    print(f'    ChatGPT抽出完了 ({chatgpt_elapsed:.1f}秒)')
                    if not prefecture:
                        prefecture = chatgpt_result.get('prefecture')
                    company_name = chatgpt_result.get('company_name', '')

                print(f'    最終都道府県: {prefecture or "(不明)"}')
                print(f'    企業名: {company_name or "(不明)"}')

                region = get_region(prefecture) if prefecture else ''

                location = extract_location_from_body(body_text)
                if not location and html_body:
                    location = extract_location_from_body(html_body)
                print(f'    勤務地抽出: {location or "(見つからず)"}')

                # 通知データを構築
                record_data = {
                    'date': mail_date_jst,
                    'title': job_title,
                    'job_type': ', '.join(job_types) if job_types else '',
                    'facility_type': facility_type,
                    'prefecture': prefecture or '',
                    'region': region,
                    'location': location,
                    'client': client_name,
                    'company_name': company_name,
                    'apply_id': apply_id,
                    'apply_url': apply_url
                }

                # 通知を送信
                print(f'    通知送信中...')
                send_notification(record_data, notify_config, instant_mode=instant_mode)

                # フラグ（スター）を付けて処理済みにする
                mb.flag(msg.uid, ['\\Flagged'], True)
                print(f'    → 完了: 通知送信・フラグ付与')
                count += 1

            # サマリー
            print(f'\n  [{client_name}] --- メール処理サマリー ---')
            print(f'  Engageメール: {len(messages)}件')
            print(f'    応募以外スキップ: {skipped_non_apply}件')
            print(f'    古いメールスキップ: {skipped_old}件')
            print(f'    職種名抽出失敗スキップ: {skipped_no_title}件')
            print(f'    新規通知: {count}件')

    except Exception as e:
        print(f'[{client_name}] メール処理エラー: {type(e).__name__}: {e}')
        print(f'[{client_name}] 詳細:\n{traceback.format_exc()}')

    elapsed = time.time() - start_time
    if count == 0:
        print(f'[{client_name}] 新着応募なし ({elapsed:.1f}秒)')
    else:
        print(f'[{client_name}] 処理完了: {count}件通知 ({elapsed:.1f}秒)')

    return count


def main():
    import traceback

    parser = argparse.ArgumentParser(description='Engage応募通知')
    parser.add_argument('--instant', action='store_true',
                        help='即時反応モード: 即時反応=TRUEのクライアントのみ処理し、LINE通知も送信')
    args = parser.parse_args()
    instant_mode = args.instant

    main_start = time.time()

    mode_label = "即時反応モード" if instant_mode else "通常モード"
    print("="*50)
    print(f"Engage応募通知 開始 ({mode_label})")
    print(f"コードバージョン: 2026-02-12d")
    print(f"実行日時: {datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S')} (JST)")
    print("="*50)

    # Google Sheets クライアント（設定読み取り用）
    print('\n[初期化] Google Sheets認証中...')
    sheets_client = get_sheets_client()
    if not sheets_client:
        print('エラー: Google Sheets連携が必要です（GOOGLE_CREDENTIALS未設定）')
        return

    print('[初期化] Google Sheets連携: 有効')

    # 通知設定を取得（即時反応モードでは不要）
    if instant_mode:
        notification_settings = {}
    else:
        print('[初期化] 通知設定取得中...')
        notification_settings = get_notification_settings(sheets_client)

    # ログイン情報をスプレッドシートから取得（IMAP列が空なら自動判定）
    print('[初期化] ログイン情報取得中...')
    credentials = get_login_credentials(sheets_client, notification_settings, instant_only=instant_mode)
    if not credentials:
        filter_label = "engage + is_active=TRUE + 即時反応=TRUE" if instant_mode else "engage + is_active=TRUE"
        print(f'エラー: 対象のログイン情報が見つかりません（{filter_label}）')
        return

    print(f'[初期化] 処理対象アカウント数: {len(credentials)}')
    for i, cred in enumerate(credentials):
        print(f'  {i+1}. {cred["client_name"]} ({cred["email"]})')

    # マッピング情報を取得
    print('[初期化] 職種マッピング取得中...')
    job_mappings = get_job_mappings(sheets_client)
    print('[初期化] 施設形態マッピング取得中...')
    facility_mappings = get_facility_mappings(sheets_client)

    init_elapsed = time.time() - main_start
    print(f'\n[初期化完了] ({init_elapsed:.1f}秒)')

    # 並列処理（2並列、アカウントごと最大120秒でタイムアウト）
    from concurrent.futures import ThreadPoolExecutor, as_completed
    MAX_WORKERS = 2
    ACCOUNT_TIMEOUT = 120
    total_count = 0

    def _process(idx, cred):
        print(f'\n{"#"*50}')
        print(f'# アカウント {idx+1}/{len(credentials)}: {cred["client_name"]}')
        print(f'{"#"*50}')
        return process_mailbox(cred, sheets_client, job_mappings, facility_mappings, instant_mode=instant_mode)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for i, credential in enumerate(credentials):
            future = executor.submit(_process, i, credential)
            futures[future] = credential

        for future in as_completed(futures):
            cred = futures[future]
            try:
                count = future.result(timeout=ACCOUNT_TIMEOUT)
                total_count += count
            except TimeoutError:
                print(f'[{cred["client_name"]}] タイムアウト ({ACCOUNT_TIMEOUT}秒) - スキップ')
            except Exception as e:
                print(f'[{cred["client_name"]}] 致命的エラー: {type(e).__name__}: {e}')

    total_elapsed = time.time() - main_start
    print(f'\n{"="*50}')
    print(f'全処理完了: 新着通知 合計 {total_count}件')
    print(f'総実行時間: {total_elapsed:.1f}秒')
    print(f'終了時刻: {datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")} (JST)')
    print(f'{"="*50}')


if __name__ == '__main__':
    _is_instant = '--instant' in sys.argv
    _script_base = os.path.dirname(os.path.abspath(__file__))
    _now = datetime.now(JST)
    _today = _now.strftime('%Y%m%d')

    if _is_instant:
        _log_dir = os.path.join(_script_base, 'logs_instant')
        _log_path = os.path.join(_log_dir, _today + '.log')
    else:
        _log_dir = os.path.join(_script_base, 'logs')
        _log_path = os.path.join(_log_dir, _now.strftime('%Y%m%d_%H%M%S') + '.log')

    # 前日以前のログを削除（保持期間1日）
    if os.path.isdir(_log_dir):
        for _f in os.listdir(_log_dir):
            if _f.endswith('.log') and not _f.startswith(_today):
                os.remove(os.path.join(_log_dir, _f))

    # 画面+ファイル両方に出力
    tee = TeeWriter(_log_path)
    sys.stdout = tee
    sys.stderr = tee
    try:
        print(f'ログファイル: {_log_path}')
        main()
    finally:
        sys.stdout = tee._stdout
        sys.stderr = tee._stdout
        tee.close()
        print(f'ログ出力完了: {_log_path}')
