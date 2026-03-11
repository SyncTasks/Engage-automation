import requests
import slackweb
import aiohttp
import asyncio
from .config import config


class NotificationManager:
    def __init__(self, config):
        self.config = config
        self.line_notify_url = "https://notify-api.line.me/api/notify"
        self.line_notify_access_token = config['line_test_notify_access_token'] if config['test_mode'] else config['line_notify_access_token']
        self.chatwork_api_token = config['chatwork_api_token']
        self.chatwork_room_id = config['chatwork_test_room_id'] if config['test_mode'] else config['chatwork_room_id']
        self.slack_url = config['slack_url']

    # 既存の同期メソッド
    def send_line_notification(self, message):
        if self.config['enable_line_notify']:
            headers = {'Authorization': 'Bearer ' + self.line_notify_access_token}
            payload = {'message': message}
            response = requests.post(self.line_notify_url, headers=headers, data=payload)
            if response.status_code == 200:
                print("Message sent to LINE Notify successfully.")
            else:
                print("Failed to send notification to LINE Notify.")
                print(response.text)

    def send_chatwork_notification(self, message):
        if self.config['enable_chatwork_notify']:
            headers = {"X-ChatWorkToken": self.chatwork_api_token}
            payload = {"body": message}
            url = f"https://api.chatwork.com/v2/rooms/{self.chatwork_room_id}/messages"
            response = requests.post(url, headers=headers, data=payload)
            if response.status_code == 200:
                print(f"Message sent to Chatwork room {self.chatwork_room_id} successfully.")
            else:
                print("Failed to send notification to Chatwork.")
                print(response.text)

    def send_slack_notification(self, message):
        if self.config['enable_slack_notify']:
            slack = slackweb.Slack(url=self.slack_url)
            slack.notify(text=message)
            print("Message sent to Slack successfully.")

    def send_notifications(self, message):
        self.send_line_notification(message)
        self.send_chatwork_notification(message)
        self.send_slack_notification(message)

    # 新しい非同期メソッド
    async def send_line_notification_async(self, message):
        if self.config['enable_line_notify']:
            headers = {'Authorization': 'Bearer ' + self.line_notify_access_token}
            payload = {'message': message}
            async with aiohttp.ClientSession() as session:
                async with session.post(self.line_notify_url, headers=headers, data=payload) as response:
                    if response.status == 200:
                        print("Message sent to LINE Notify successfully.")
                    else:
                        print("Failed to send notification to LINE Notify.")
                        print(await response.text())

    async def send_chatwork_notification_async(self, message):
        if self.config['enable_chatwork_notify']:
            headers = {"X-ChatWorkToken": self.chatwork_api_token}
            payload = {"body": message}
            url = f"https://api.chatwork.com/v2/rooms/{self.chatwork_room_id}/messages"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, data=payload) as response:
                    if response.status == 200:
                        print(f"Message sent to Chatwork room {self.chatwork_room_id} successfully.")
                    else:
                        print("Failed to send notification to Chatwork.")
                        print(await response.text())

    async def send_slack_notification_async(self, message):
        if self.config['enable_slack_notify']:
            await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: slackweb.Slack(url=self.slack_url).notify(text=message)
            )
            print("Message sent to Slack successfully.")

    async def send_notifications_async(self, message):
        await asyncio.gather(
            self.send_line_notification_async(message),
            self.send_chatwork_notification_async(message),
            self.send_slack_notification_async(message)
        )


# テスト実行関数
async def run_tests():
    notification_manager = NotificationManager(config)
    test_message = "これはテストメッセージです。"

    # print("同期メソッドのテスト:")
    # notification_manager.send_notifications(test_message)

    print("\n非同期メソッドのテスト:")
    await notification_manager.send_notifications_async(test_message)

if __name__ == "__main__":
    asyncio.run(run_tests())
