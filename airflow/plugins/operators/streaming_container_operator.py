import time

import docker

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class StreamingContainerOperator(BaseOperator):

    def __init__(
        self,
        container_name: str,
        action: str,
        check_interval: int = 2,
        wait_timeout: int = 20,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.container_name = container_name
        self.action = action
        self.check_interval = check_interval
        self.wait_timeout = wait_timeout

    def execute(self, context):
        if self.action not in ("start", "stop"):
            raise AirflowException(
                f"Unsupported action: {self.action}"
            )

        client = docker.from_env()

        if self.action == "start":
            self._execute_start(client)
        else:
            self._execute_stop(client)

    def _execute_start(self, client):
        try:
            container = client.containers.get(
                self.container_name
            )

        except docker.errors.NotFound as exc:
            raise AirflowException(
                f"Container not found: "
                f"{self.container_name}. "
                "Cannot start streaming."
            ) from exc

        container.reload()
        status = container.status

        self.log.info(
            "Start requested. "
            "container=%s, current_status=%s",
            self.container_name,
            status,
        )

        if status == "running":
            self.log.info(
                "%s is already running.",
                self.container_name,
            )
            return

        if status in ("exited", "created"):
            self.log.info(
                "Starting %s...",
                self.container_name,
            )

            container.start()

            self._wait_for_status(
                container=container,
                expected_status="running",
            )
            return

        if status == "restarting":
            self.log.warning(
                "%s is restarting. "
                "Waiting for running state...",
                self.container_name,
            )

            self._wait_for_status(
                container=container,
                expected_status="running",
            )
            return

        if status == "dead":
            raise AirflowException(
                f"{self.container_name} "
                "is in dead state. "
                "Streaming state cannot be trusted."
            )

        raise AirflowException(
            f"Cannot start {self.container_name}. "
            f"Unexpected container status={status}"
        )

    def _execute_stop(self, client):
        try:
            container = client.containers.get(
                self.container_name
            )

        except docker.errors.NotFound:
            self.log.warning(
                "%s container not found. "
                "Streaming is not running, "
                "so batch processing can continue.",
                self.container_name,
            )
            return

        container.reload()
        status = container.status

        self.log.info(
            "Stop requested. "
            "container=%s, current_status=%s",
            self.container_name,
            status,
        )

        if status in ("exited", "created"):
            self.log.info(
                "%s is already not running. "
                "status=%s",
                self.container_name,
                status,
            )
            return

        if status in (
            "running",
            "restarting",
            "paused",
        ):
            self.log.info(
                "Stopping %s. "
                "current_status=%s",
                self.container_name,
                status,
            )

            container.stop()

            self._wait_for_status(
                container=container,
                expected_status="exited",
            )
            return

        if status == "removing":
            self.log.warning(
                "%s is being removed. "
                "Waiting for container removal...",
                self.container_name,
            )

            self._wait_until_not_found(client)
            return

        if status == "dead":
            raise AirflowException(
                f"{self.container_name} "
                "is in dead state. "
                "Streaming state cannot be trusted, "
                "so batch processing will not continue."
            )

        raise AirflowException(
            f"Cannot stop {self.container_name}. "
            f"Unexpected container status={status}"
        )

    def _wait_for_status(
        self,
        container,
        expected_status: str,
    ):
        elapsed = 0

        while elapsed < self.wait_timeout:
            container.reload()
            status = container.status

            self.log.info(
                "Waiting for container state. "
                "container=%s, "
                "current_status=%s, "
                "expected_status=%s",
                self.container_name,
                status,
                expected_status,
            )

            if status == expected_status:
                self.log.info(
                    "%s reached expected status=%s",
                    self.container_name,
                    expected_status,
                )
                return

            if status == "dead":
                raise AirflowException(
                    f"{self.container_name} "
                    "entered dead state while "
                    f"waiting for {expected_status}."
                )

            time.sleep(
                self.check_interval
            )

            elapsed += (
                self.check_interval
            )

        raise AirflowException(
            f"{self.container_name} "
            f"did not reach {expected_status} "
            f"within {self.wait_timeout} seconds."
        )

    def _wait_until_not_found(
        self,
        client,
    ):
        elapsed = 0

        while elapsed < self.wait_timeout:
            try:
                container = (
                    client.containers.get(
                        self.container_name
                    )
                )

                container.reload()

                self.log.info(
                    "Waiting for container removal. "
                    "container=%s, "
                    "current_status=%s",
                    self.container_name,
                    container.status,
                )

                if container.status == "dead":
                    raise AirflowException(
                        f"{self.container_name} "
                        "entered dead state while "
                        "being removed."
                    )

            except docker.errors.NotFound:
                self.log.info(
                    "%s removal completed.",
                    self.container_name,
                )
                return

            time.sleep(
                self.check_interval
            )

            elapsed += (
                self.check_interval
            )

        raise AirflowException(
            f"{self.container_name} "
            "was not removed within "
            f"{self.wait_timeout} seconds."
        )