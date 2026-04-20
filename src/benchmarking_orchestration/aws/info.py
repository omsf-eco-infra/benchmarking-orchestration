import requests
from functools import cache


class MetadataService:
    BASE = "http://169.254.169.254/latest"

    def __init__(self, timeout: int = 1):
        self.timeout = timeout
        self.session = requests.Session()

        try:
            resp = self.session.put(
                f"{self.BASE}/api/token",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            token = resp.text
            self.headers = {"X-aws-ec2-metadata-token": token}
        except requests.RequestException:
            # Fallback to IMDSv1 if allowed
            self.headers = {}

    def _get(self, path: str) -> str:
        resp = self.session.get(
            f"{self.BASE}/meta-data/{path}",
            headers=self.headers,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.text

    @cache
    def instance_type(self) -> str:
        return self._get("instance-type")

    @cache
    def instance_id(self) -> str:
        return self._get("instance-id")

    @cache
    def ami_id(self) -> str:
        return self._get("ami-id")
