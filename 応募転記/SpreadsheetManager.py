# SpreadsheetManager.py
import os
from google.oauth2.service_account import Credentials
import gspread


# 新しいスプレッドシート設定
NEW_SPREADSHEET_ID = '1_8IZ_q8ezL5VY6DBmDr8tXuTKU5nTaZ8MU38lWL7Vpw'
NEW_SHEET_NAME = '応募者シート'

# 旧スプレッドシート設定（参照用に保持）
OLD_SPREADSHEET_ID = '1UQk_YkjIFSrg1hSRRYLsO9bHwZ0xGSrxC3fFB62pONo'


class SpreadsheetManager:
    def __init__(self, credentials_path, spreadsheet_key=None, sheet_name=None):
        self.credentials_path = credentials_path
        # 新しいスプレッドシートをデフォルトに
        self.spreadsheet_key = spreadsheet_key or NEW_SPREADSHEET_ID
        self.sheet_name = sheet_name or NEW_SHEET_NAME
        self.client = self._authenticate()
        self.sheet = self._get_sheet()

    def _authenticate(self):
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = Credentials.from_service_account_file(
            self.credentials_path, scopes=scopes
        )
        return gspread.authorize(credentials)

    def _get_sheet(self):
        spreadsheet = self.client.open_by_key(self.spreadsheet_key)
        return spreadsheet.worksheet(self.sheet_name)

    def write_data(self, data):
        # 新しいカラム構成（26列）
        headers = [
            "クライアント", "職種", "応募日時", "都道府県", "エリア", "名前", "年齢",
            "応募先求人（URL）", "施設形態", "施設形態詳細", "ふりがな", "メールアドレス",
            "電話番号", "生年月日", "性別", "住所", "タイトル", "クライアント名", "備考",
            "pdfURL", "アカウントID", "応募者ID", "割り当て", "集計状況", "媒体",
            "応募先企業名", "", ""
        ]

        # Prepare the row data
        row_data = [
            data.get(header, "") for header in headers
        ]

        # USER_ENTEREDで書き込み（数値・日付を適切に解釈させる）
        self.sheet.append_row(row_data, value_input_option='USER_ENTERED')

        # 郵便番号をヘッダー名で列を探して書き込み
        postal_code = data.get("郵便番号", "")
        if postal_code:
            header_row = self.sheet.row_values(1)
            if "郵便番号" in header_row:
                col_index = header_row.index("郵便番号") + 1  # 1-based
                last_row = len(self.sheet.get_all_values())
                self.sheet.update_cell(last_row, col_index, postal_code)

    @classmethod
    def get_existing_ids(cls):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        credentials_path = os.path.join(current_dir, "Credentials.json")
        spreadsheet_key = '1UQk_YkjIFSrg1hSRRYLsO9bHwZ0xGSrxC3fFB62pONo'

        writer = cls(credentials_path, spreadsheet_key)
        all_values = writer.sheet.get_all_values()
        
        if len(all_values) > 1:  # Check if there's data besides the header
            return [row[0] for row in all_values[1:]]  # Assuming ID is the first column
        return []
    @classmethod
    def check_existing_id(cls, id_to_check):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        credentials_path = os.path.join(current_dir, "Credentials.json")
        spreadsheet_key = '1UQk_YkjIFSrg1hSRRYLsO9bHwZ0xGSrxC3fFB62pONo'

        manager = cls(credentials_path, spreadsheet_key)
        all_values = manager.sheet.get_all_values()
        
        if len(all_values) > 1:
            headers = all_values[0]
            try:
                id_index = headers.index("ID")
                existing_ids = [row[id_index] for row in all_values[1:]]
                return id_to_check in existing_ids
            except ValueError:
                print("'ID'カラムが見つかりません")
                return False
        return False
        
    @classmethod
    def check_existing_email(cls, email_to_check):
        """後方互換性のために残す（非推奨）"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        credentials_path = os.path.join(current_dir, "Credentials.json")

        manager = cls(credentials_path)
        all_values = manager.sheet.get_all_values()

        if len(all_values) > 1:
            headers = all_values[0]
            try:
                email_index = headers.index("メールアドレス")
                existing_emails = [row[email_index] for row in all_values[1:]]
                return email_to_check in existing_emails
            except ValueError:
                print("'メールアドレス'カラムが見つかりません")
                return False
        return False

    @classmethod
    def check_duplicate_application(cls, email_to_check, job_url_to_check):
        """
        メールアドレスと求人URLの両方が一致するレコードがあるかチェック
        同一求人に同一人物が応募した場合は重複とみなす
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        credentials_path = os.path.join(current_dir, "Credentials.json")

        manager = cls(credentials_path)
        all_values = manager.sheet.get_all_values()

        if len(all_values) > 1:
            headers = all_values[0]
            try:
                email_index = headers.index("メールアドレス")
                job_url_index = headers.index("応募先求人（URL）")

                for row in all_values[1:]:
                    existing_email = row[email_index] if email_index < len(row) else ""
                    existing_job_url = row[job_url_index] if job_url_index < len(row) else ""

                    if existing_email == email_to_check and existing_job_url == job_url_to_check:
                        return True
                return False
            except ValueError as e:
                print(f"必要なカラムが見つかりません: {e}")
                return False
        return False



class DataProcessor:
    def __init__(self, scraper_data):
        self.scraper_data = scraper_data

    def _get_value(self, key, default=""):
        """値を取得し、「情報なし」は空欄に変換"""
        value = self.scraper_data.get(key, default)
        if value == "情報なし":
            return ""
        return value

    def _format_datetime(self, value):
        """日時を 'YYYY-MM-DD H:MM:SS' 形式に変換"""
        if not value or value == "情報なし":
            return ""
        import re
        # 既にハイフン形式の場合はそのまま返す
        if re.match(r'^\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{2}$', value):
            return value
        # 「2026年1月28日 23:51」のような形式を変換
        match = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日\s*(\d{1,2}):(\d{2})', value)
        if match:
            year, month, day, hour, minute = match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d} {int(hour)}:{int(minute):02d}:00"
        # 「2026/1/28 23:51」のような形式を変換
        match = re.match(r'(\d{4})/(\d{1,2})/(\d{1,2})\s*(\d{1,2}):(\d{2})', value)
        if match:
            year, month, day, hour, minute = match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d} {int(hour)}:{int(minute):02d}:00"
        # 「2026-01-28 23:51:00」のような形式を変換
        match = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})\s*(\d{1,2}):(\d{2}):?(\d{2})?', value)
        if match:
            year, month, day, hour, minute, sec = match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d} {int(hour)}:{int(minute):02d}:{int(sec or 0):02d}"
        # 「2026/2/16」のような日付のみ（時刻なし）を変換
        match = re.match(r'(\d{4})/(\d{1,2})/(\d{1,2})$', value.strip())
        if match:
            year, month, day = match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d} 0:00:00"
        # 「2026年2月16日」のような日付のみ（時刻なし）を変換
        match = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日$', value.strip())
        if match:
            year, month, day = match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d} 0:00:00"
        return value

    def _format_age(self, value):
        """年齢から「歳」を削除し数値として返す"""
        if not value or value == "情報なし":
            return ""
        age_str = value.replace("歳", "").strip()
        try:
            return int(age_str)
        except ValueError:
            return age_str

    def _format_birthday(self, value):
        """生年月日を 'YYYY-M-D' 形式に変換"""
        if not value or value == "情報なし":
            return ""
        import re
        # 「1967年2月21日」のような形式を変換
        match = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', value)
        if match:
            year, month, day = match.groups()
            return f"{year}-{int(month)}-{int(day)}"
        # 「1967/2/21」のような形式を変換
        match = re.match(r'(\d{4})/(\d{1,2})/(\d{1,2})', value)
        if match:
            year, month, day = match.groups()
            return f"{year}-{int(month)}-{int(day)}"
        # 「1967-02-21」のような形式を変換（ゼロパディング除去）
        match = re.match(r'^\'?(\d{4})-(\d{1,2})-(\d{1,2})$', value)
        if match:
            year, month, day = match.groups()
            return f"{year}-{int(month)}-{int(day)}"
        return value

    def process_data(self):
        client_name = self._get_value("クライアント")

        # 新しいカラム構成（27列）に対応
        processed_data = {
            "クライアント": client_name,
            "職種": self._get_value("職種"),
            "応募日時": f"'{self._format_datetime(self._get_value('応募日時'))}" if self._format_datetime(self._get_value('応募日時')) else "",
            "都道府県": self._get_value("都道府県"),
            "エリア": self._get_value("エリア"),
            "名前": self._get_value("名前"),
            "年齢": self._format_age(self._get_value("年齢")),
            "応募先求人（URL）": self._get_value("求人URL"),
            "施設形態": self._get_value("施設形態"),
            "施設形態詳細": self._get_value("施設形態詳細"),
            "ふりがな": self._get_value("ふりがな"),
            "メールアドレス": self._get_value("メールアドレス"),
            "電話番号": f"'{self._get_value('電話番号')}" if self._get_value('電話番号') else "",
            "生年月日": self._format_birthday(self._get_value("生年月日")),
            "性別": self._get_value("性別"),
            "住所": self._get_value("住所"),
            "郵便番号": self._get_value("郵便番号"),
            "タイトル": self._get_value("タイトル"),
            "クライアント名": client_name,  # クライアントと同じ値
            "備考": "",
            "pdfURL": "",
            "アカウントID": self._get_value("アカウントID"),
            "応募者ID": self._get_value("応募者ID"),
            "割り当て": "",
            "集計状況": "",  # 空欄
            "媒体": "Engage",  # 固定値
            "応募先企業名": "",  # 空欄
            "": ""  # 末尾の空欄カラム
        }
        return processed_data


def write_row(self, data):
    self.write_data(data)


def write_to_spreadsheet(scraper_data):
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        credentials_path = os.path.join(current_dir, "Credentials.json")

        # 新しいスプレッドシートに書き込み
        writer = SpreadsheetManager(credentials_path)

        if isinstance(scraper_data, dict):
            scraper_data_list = [scraper_data]
        elif isinstance(scraper_data, list):
            scraper_data_list = scraper_data
        else:
            raise ValueError("scraper_data must be a dictionary or a list of dictionaries")

        for data in scraper_data_list:
            processor = DataProcessor(data)
            processed_data = processor.process_data()
            writer.write_data(processed_data)

        print("スプレッドシートに書き込み完了")
    except Exception as e:
        print(f"An error occurred while writing to the spreadsheet: {str(e)}")





def get_engage_data():
    # Google Sheets APIの認証設定
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(current_dir, "Credentials.json")
    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    client = gspread.authorize(credentials)
    
    # スプレッドシートを開く
    spreadsheet_id = '1UQk_YkjIFSrg1hSRRYLsO9bHwZ0xGSrxC3fFB62pONo'
    spreadsheet = client.open_by_key(spreadsheet_id)
    
    # アクセスするシートを「アイパスマスタ」に変更
    ipass_master_sheet = spreadsheet.worksheet("アイパスマスタ")
    
    # 全データを取得
    all_values = ipass_master_sheet.get_all_values()
    
    if not all_values:
        return []

    # ヘッダーを取得
    headers = all_values[0]
    
    # 必要なカラムのインデックスを取得
    try:
        media_name_index = headers.index("媒体名")
        client_name_index = headers.index("クライアント名")
        email_index = headers.index("メール")
        password_index = headers.index("パス")
    except ValueError as e:
        print(f"必要なカラムが見つかりません: {e}")
        return []

    # "媒体名"が"engage"または"エンゲージ"の行を抽出
    engage_data = []
    for row in all_values[1:]:
        if row[media_name_index].lower() == "engage" or row[media_name_index] == "エンゲージ":
            engage_data.append({
                "クライアント名": row[client_name_index],
                "メール": row[email_index],
                "パス": row[password_index]
            })
    return engage_data

if __name__ == "__main__":
    get_engage_data()