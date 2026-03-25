"""
即時通知用スクレイパー - 応募通知からサブプロセスで呼び出されるラッパー

コマンドライン:
  python instant_scraper.py --client-name "X" --email "X" --password "X"

応募転記のvenv環境で実行される前提。
結果は ###RESULT### マーカー付きでJSON出力。
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime

# 応募転記ディレクトリをパスに追加
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from engage_check_apply import (
    User, login_to_website, process_single_row, close_modal_if_exists,
    wait_for_element, human_delay, logout, print_log,
)
from SpreadsheetManager import SpreadsheetManager, write_to_spreadsheet

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth


EXECUTION_ENV = "VPS4号_ Engage-automation_即時通知"


def output_result(result: dict):
    """結果をマーカー付きでstdoutに出力"""
    print(f"###RESULT###{json.dumps(result, ensure_ascii=True)}###RESULT###", flush=True)


async def run_instant_scraper(client_name: str, email: str, password: str, max_count: int = 0) -> dict:
    """即時スクレイピングを実行"""
    result = {
        "success": False,
        "written_count": 0,
        "applicants": [],
        "error": None,
    }

    user = User(
        media_name="エンゲージ",
        client_name=client_name,
        user_id=email,
        password=password,
        is_active=True,
    )

    profile_dir = os.path.join(CURRENT_DIR, "chrome_profile_instant")
    context = None

    # 残存Chromeプロセスのクリーンアップ（instant用プロファイルのロックのみ）
    for lock_file in ["SingletonLock", "SingletonSocket", "SingletonCookie", "lockfile"]:
        lock_path = os.path.join(profile_dir, lock_file)
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
                print_log(f"ロックファイルを削除: {lock_file}")
            except Exception:
                pass

    try:
        async with async_playwright() as playwright:
            print_log(f"即時スクレイパー開始: {client_name}")

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

            # ログイン
            if not await login_to_website(page, user):
                result["error"] = "ログイン失敗"
                print_log(f"即時スクレイパー: {client_name} ログイン失敗")
                return result

            print_log(f"即時スクレイパー: {client_name} ログイン成功")
            await human_delay(1000, 2000)

            consecutive_failures = 0
            MAX_CONSECUTIVE_FAILURES = 3

            processed_count = 0

            while True:
                # 件数制限チェック
                if max_count > 0 and processed_count >= max_count:
                    print_log(f"{client_name}: 最大処理件数({max_count})に到達")
                    break

                # 「新着の応募はありません」チェック
                no_data_element = await page.query_selector('#js_applicantNoData')
                if no_data_element:
                    is_visible = await no_data_element.is_visible()
                    if is_visible:
                        print_log(f"{client_name}: 新着の応募はありません")
                        break

                row_selector = 'tbody#js_applicantList tr[data-seq="1"]'
                row_element = await wait_for_element(page, row_selector, timeout=5000)

                if not row_element:
                    print_log(f"{client_name}: 新規データなし")
                    break

                print_log(f"{client_name}: 新規データを処理中")

                try:
                    data = await process_single_row(page, row_element, client_name, email, context)
                    if data:
                        consecutive_failures = 0

                        # 実行環境を追加
                        data["実行環境"] = EXECUTION_ENV

                        email_addr = data.get('メールアドレス', '')
                        job_url = data.get('求人URL', '')

                        # 重複チェック
                        if not SpreadsheetManager.check_duplicate_application(email_addr, job_url):
                            # スプレッドシートに書き込み（通知はスキップ）
                            write_to_spreadsheet(data)
                            result["written_count"] += 1
                            result["applicants"].append({
                                "名前": data.get("名前", ""),
                                "年齢": data.get("年齢", ""),
                                "性別": data.get("性別", ""),
                                "電話番号": data.get("電話番号", ""),
                                "メールアドレス": data.get("メールアドレス", ""),
                                "職種": data.get("職種", ""),
                                "タイトル": data.get("タイトル", ""),
                                "住所": data.get("住所", ""),
                                "応募日時": data.get("応募日時", ""),
                                "求人URL": data.get("求人URL", ""),
                                "クライアント": data.get("クライアント", ""),
                            })
                            print_log(f"転記完了: {data.get('名前', '不明')}")
                            processed_count += 1
                        else:
                            print_log(f"重複スキップ: {email_addr} / {job_url}")
                            processed_count += 1
                    else:
                        consecutive_failures += 1
                        print_log(f"データ取得失敗（連続: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}）")
                        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                            result["error"] = f"連続{MAX_CONSECUTIVE_FAILURES}回データ取得失敗"
                            break

                except PlaywrightTimeoutError as e:
                    consecutive_failures += 1
                    print_log(f"タイムアウト（連続: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}）: {e}")
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        result["error"] = f"タイムアウト: {str(e)}"
                        break

                except Exception as e:
                    consecutive_failures += 1
                    print_log(f"エラー（連続: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}）: {type(e).__name__}: {e}")
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        result["error"] = f"{type(e).__name__}: {str(e)}"
                        break

                # 管理画面に戻る
                try:
                    await page.goto("https://en-gage.net/company/manage/", wait_until='domcontentloaded', timeout=30000)
                except PlaywrightTimeoutError:
                    print_log("ページ遷移タイムアウト（続行）")
                await human_delay(1000, 2000)

            # ログアウト
            try:
                await logout(page)
            except Exception as e:
                print_log(f"ログアウトエラー: {e}")

            result["success"] = True

    except Exception as e:
        result["error"] = f"{type(e).__name__}: {str(e)}"
        print_log(f"即時スクレイパー致命的エラー: {result['error']}")

    finally:
        if context:
            try:
                await context.close()
            except Exception:
                pass

    return result


def main():
    parser = argparse.ArgumentParser(description='即時通知用Engageスクレイパー')
    parser.add_argument('--client-name', required=True, help='クライアント名')
    parser.add_argument('--email', required=True, help='ログインメールアドレス')
    parser.add_argument('--password', required=True, help='ログインパスワード')
    parser.add_argument('--max-count', type=int, default=0, help='最大処理件数 (0=無制限)')
    args = parser.parse_args()

    print_log(f"即時スクレイパー起動: {args.client_name}")

    result = asyncio.run(run_instant_scraper(args.client_name, args.email, args.password, max_count=args.max_count))

    output_result(result)
    print_log(f"即時スクレイパー終了: success={result['success']}, written={result['written_count']}")


if __name__ == "__main__":
    main()
