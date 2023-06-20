import requests
import logging
from datetime import datetime, timedelta, timezone
from requests.auth import HTTPBasicAuth

logger = logging.getLogger("app_log")


class TwitterClient:
    def __init__(self, config):
        self.config = config
        self._bearer_token = None
        self._base_url = "https://api.twitter.com"

    def _get_bearer_token(self):
        if self._bearer_token is not None:
            return self._bearer_token
        res = requests.post(
            f'{self._base_url}/oauth2/token?grant_type=client_credentials',
            auth=HTTPBasicAuth(self.config.TWITTER_API_KEY, self.config.TWITTER_API_SECRET)
        )
        token = res.json()["access_token"]
        self._bearer_token = token
        return token

    def _construct_headers(self):
        bearer_token = self._get_bearer_token()
        return {"Authorization": f"Bearer {bearer_token}"}


    def get_users_by_username(self, user_names: list):
        headers = self._construct_headers()
        username_param = ",".join(user_names)
        response = requests.get(f"{self._base_url}/2/users/by?usernames={username_param}", headers=headers)
        return response.json()["data"]

    def get_user_timeline(self, user_id: int, start: str, end: str):
        headers = self._construct_headers()
        params = f"start_time={start}&end_time={end}&tweet.fields=public_metrics,created_at&max_results=100"
        response = requests.get(f"{self._base_url}/2/users/{user_id}/tweets?{params}", headers=headers)
        if response.status_code == 200:
            data = response.json().get("data", [])
            return data
        logger.error(f"Status Code: {response.status_code}\nError Data: {response.json()}")

    def get_user_timeline_n_days_ago(self, user_id: int, n: int):
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=n)
        end_str = end.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_str = start.strftime('%Y-%m-%dT%H:%M:%SZ')
        return self.get_user_timeline(user_id, start_str, end_str)
