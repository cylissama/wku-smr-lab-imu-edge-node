import signal
import time
import socket #added by umar khattab 
from imu.DataWriter import DataWriter
from imu.service_contract import ServiceError, SessionRequest

from .config import EdgeAgentConfig
from .health import EdgeHealthTracker
from .service_client import LocalIMUServiceClient


class EdgeAgent:
    def __init__(self, config: EdgeAgentConfig):
        self.config = config
        self.client = LocalIMUServiceClient(config.socket_path)
        self.health = EdgeHealthTracker(config.health_path)
        self._stop_requested = False
        self._triggered=False #added by umar
        self.trigger_time_ms: int | None= None  #added by umar

    def install_signal_handlers(self) -> None:
        def _handle_signal(_signum, _frame):
            self._stop_requested = True

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

    def run(self) -> None:
        self.install_signal_handlers()

        with DataWriter(
            csv_fname=self.config.csv_path or DataWriter.DEFAULT_CSV_FNAME,
            mqtt_broker_ip=self.config.mqtt_broker_ip,
            mqtt_broker_port=self.config.mqtt_broker_port,
            device_id=self.config.device_id,
        ) as writer:
            while not self._stop_requested:
                try:
                    self._wait_until_ready()
                    if self.config.wait_for_trigger and not self._triggered: #added by umar until break command.
                        self._wait_for_trigger()
                        if self._stop_requested:
                            break
                    if self.config.start_session_on_boot:
                        self._start_session()

                    with self.client.stream() as stream:
                        for sample in stream:
                            if self._stop_requested:
                                break

                            writer.write_data(sample)
                            self.health.update(
                                state="streaming",
                                session_id=self.config.session_id,
                                last_capture_time_ms=sample.capture_time_ms,
                                last_counter=sample.counter,
                            )
                except ServiceError as error:
                    self.health.update(
                        state="degraded",
                        error_code=error.code,
                        error_message=error.message,
                    )
                    time.sleep(self.config.reconnect_delay_s)
                except Exception as error:
                    self.health.update(
                        state="degraded",
                        error_code="EDGE_FAILURE",
                        error_message=str(error),
                    )
                    time.sleep(self.config.reconnect_delay_s)

        if self.config.stop_session_on_exit:
            try:
                self.client.stop_session()
            except ServiceError:
                pass

    def _wait_until_ready(self) -> None:
        while not self._stop_requested:
            try:
                ready = self.client.readiness()
            except OSError as error:
                self.health.update(
                    state="waiting",
                    error_code="SERVICE_UNAVAILABLE",
                    error_message=str(error),
                )
                time.sleep(self.config.reconnect_delay_s)
                continue
            except ServiceError as error:
                self.health.update(
                    state="waiting",
                    error_code=error.code,
                    error_message=error.message,
                )
                time.sleep(self.config.reconnect_delay_s)
                continue

            if ready.get("ready"):
                return

            time.sleep(self.config.reconnect_delay_s)

    def _wait_for_trigger(self) -> None:
        """
added by umar khattab, to test the udp part
        Block until the robot/PLC sends the UDP start signal, then pause so the
        arm settles. Nothing is tared, streamed or published before this returns.
        """
        expected = self.config.trigger_payload.strip().encode()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self.config.trigger_port))
        sock.settimeout(0.5)
        last_health = 0.0

        try:
            while not self._stop_requested:
                # Keep the health file fresh so the container stays healthy
                # however long the robot takes to send the signal.
                if time.monotonic() - last_health > 5.0:
                    self.health.update(
                        state="waiting",
                        detail=f"waiting for UDP trigger on port {self.config.trigger_port}",
                    )
                    last_health = time.monotonic()

                try:
                    data, _addr = sock.recvfrom(1024)
                except socket.timeout:
                    continue

                if self._is_trigger(data, expected):
                    self.trigger_time_ms = int(time.time_ns() / 1e6)
                    self._triggered = True
                    break
        finally:
            sock.close()

        if self._triggered and self.config.trigger_settle_s > 0:
            time.sleep(self.config.trigger_settle_s)

    @staticmethod
    def _is_trigger(data: bytes, expected: bytes) -> bool:
        """
added by umar khattab, to test the udp part
        True when a packet carries the start signal: the configured text
        (default "1"), or the number 1 sent by a PLC as a 1-, 2- or 4-byte
        integer in either byte order. A 0 or anything else is ignored.
        """
        if data.strip() == expected:
            return True
        if len(data) in (1, 2, 4):
            return 1 in (int.from_bytes(data, "big"), int.from_bytes(data, "little"))
        return False

    def _start_session(self) -> None:
        request = SessionRequest(
            session_id=self.config.session_id,
            sample_hz=self.config.sample_hz,
            tare=self.config.request_tare,
            reset_counter=self.config.reset_counter,
        )

        try:
            self.client.start_session(request)
        except ServiceError as error:
            if error.code != "SESSION_ALREADY_ACTIVE":
                raise
            if self.config.request_tare(): #this if statement has been added by umar khattab
                self.client.tare()
