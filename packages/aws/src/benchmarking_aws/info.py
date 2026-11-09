"""EC2 Instance Metadata Service access."""

from functools import cache

import requests


class MetadataService:
    """Read instance identity from the EC2 Instance Metadata Service."""

    BASE = "http://169.254.169.254/latest"

    def __init__(self, timeout: int = 1):
        """Initialize an IMDS session, preferring IMDSv2.

        Parameters
        ----------
        timeout : int, default=1
            Timeout in seconds for each metadata request.
        """
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
        """Fetch an instance metadata value.

        Parameters
        ----------
        path : str
            Metadata path relative to ``meta-data/``.

        Returns
        -------
        str
            Metadata response text.
        """
        resp = self.session.get(
            f"{self.BASE}/meta-data/{path}",
            headers=self.headers,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.text

    @cache
    def instance_type(self) -> str:
        """Return the EC2 instance type.

        Returns
        -------
        str
            Instance type, such as ``g5.xlarge``.
        """
        return self._get("instance-type")

    @cache
    def instance_id(self) -> str:
        """Return the EC2 instance identifier.

        Returns
        -------
        str
            Identifier of the current instance.
        """
        return self._get("instance-id")

    @cache
    def ami_id(self) -> str:
        """Return the machine image identifier.

        Returns
        -------
        str
            AMI used to launch the current instance.
        """
        return self._get("ami-id")
