import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

config = {
    # テストモード設定
    'test_mode': False,  # True for test mode, False for production

    # LINE Notify設定
    'enable_line_notify': True,
    'line_notify_access_token': os.getenv('LINE_NOTIFY_ACCESS_TOKEN', ''),
    'line_test_notify_access_token': os.getenv('LINE_TEST_NOTIFY_ACCESS_TOKEN', ''),

    # Chatwork設定
    'enable_chatwork_notify': True,
    'chatwork_api_token': os.getenv('CHATWORK_API_TOKEN', ''),
    'chatwork_room_id': os.getenv('CHATWORK_ROOM_ID', ''),
    'chatwork_test_room_id': os.getenv('CHATWORK_TEST_ROOM_ID', ''),

    # Slack設定
    'enable_slack_notify': True,
    'slack_url': os.getenv('SLACK_WEBHOOK_URL', ''),
}
