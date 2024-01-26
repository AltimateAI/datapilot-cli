import requests


class APIClient:
    def __init__(self, api_token, base_url, tenant):
        self.api_token = api_token
        self.base_url = base_url
        self.tenant = tenant

    def _get_headers(self):
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "X-Tenant": self.tenant,
        }
        return headers

    def get(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        headers = self._get_headers()

        response = requests.get(url, headers=headers, params=params)

        return response.json() if response.status_code == 200 else None

    def post(self, endpoint, data=None):
        url = f"{self.base_url}/{endpoint}"
        headers = self._get_headers()

        response = requests.post(url, headers=headers, json=data)

        return response.json() if response.status_code == 201 else None
