import signal
import time

from imu.DataWriter import DataWriter
from imu.service_contract import ServiceError, SessionRequest

from .config import EdgeAgentConfig
from .health import EdgeHealthTracker
from .plc_signals import PLCSignalWatcher, build_plc_factory
from .service_client import LocalIMUServiceClient


class EdgeAgent:
    def __init__(self, config: EdgeAgentConfig):
        self.config = config
        self.client = LocalIMUServiceClient(config.socket_path)
        self.health = EdgeHealthTracker(config.health_path)
        self._stop_requested = False

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
            if self.config.plc_trigger:
                self._run_plc_triggered(writer)
                return

            while not self._stop_requested:
                try:
                    self._wait_until_ready()
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

    def _run_plc_triggered(self, writer) -> None:
        """
        Let the robot cell's PLC drive each run. The PLC sends short pulses,
        so only a tag turning ON matters; turning OFF is ignored:

            Data_Start turns ON  -> start: stream and send IMU data
            IMU_Tare turns ON    -> tare, keep sending
            Db_Stop turns ON     -> stop sending, wait for the next Data_Start
        """
        watcher = PLCSignalWatcher(
            build_plc_factory(
                self.config.plc_path,
                fake=self.config.plc_fake,
                fake_speed=self.config.plc_fake_speed,
            ),
            tag_start=self.config.plc_tag_start,
            tag_tare=self.config.plc_tag_tare,
            tag_stop=self.config.plc_tag_stop,
            poll_s=self.config.plc_poll_s,
        )
        watcher.start()

        try:
            while not self._stop_requested:
                try:
                    self._wait_until_ready()
                    if not self._wait_for_plc_start(watcher):
                        break
                    self._capture_one_run(watcher, writer)
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
        finally:
            watcher.close()
            try:
                self.client.stop_session()
            except (ServiceError, OSError):
                pass

    def _wait_for_plc_start(self, watcher: PLCSignalWatcher) -> bool:
        """Block until Data_Start turns ON. False if the agent is shutting down."""
        starts_seen = watcher.start_count
        last_health = 0.0
        while not self._stop_requested:
            if watcher.start_count > starts_seen:
                return True

            # Keep the health file fresh so the container stays healthy
            # however long the robot takes to start.
            if time.monotonic() - last_health > 5.0:
                detail = f"waiting for PLC tag {self.config.plc_tag_start}"
                if not watcher.connected:
                    detail = f"cannot reach PLC at {self.config.plc_path}: {watcher.last_error}"
                elif watcher.last_error:
                    detail = f"PLC tag problem: {watcher.last_error}"
                self.health.update(state="waiting", detail=detail)
                last_health = time.monotonic()

            time.sleep(self.config.plc_poll_s)
        return False

    def _capture_one_run(self, watcher: PLCSignalWatcher, writer) -> None:
        tares_seen = watcher.tare_count
        stops_seen = watcher.stop_count
        run_started = time.monotonic()
        max_run_s = self.config.plc_max_run_s
        first_counter: int | None = None
        sent = 0
        tares = 0
        end_reason = "the IMU service stopped sending data"

        # The tare comes later, from the PLC signal, not at session start.
        try:
            self.client.start_session(
                SessionRequest(
                    session_id=self.config.session_id,
                    sample_hz=self.config.sample_hz,
                    tare=False,
                    reset_counter=self.config.reset_counter,
                )
            )
        except ServiceError as error:
            if error.code != "SESSION_ALREADY_ACTIVE":
                raise
        print(
            f"PLC: {self.config.plc_tag_start} ON -> streaming and sending"
            f"   [{watcher.snapshot()}]",
            flush=True,
        )

        try:
            with self.client.stream() as stream:
                for sample in stream:
                    if self._stop_requested:
                        end_reason = "the agent was stopped"
                        break
                    if watcher.stop_count > stops_seen:
                        end_reason = f"{self.config.plc_tag_stop} turned ON"
                        break
                    if max_run_s > 0 and time.monotonic() - run_started > max_run_s:
                        end_reason = (
                            f"no {self.config.plc_tag_stop} signal within "
                            f"{max_run_s:g} s (IMU_PLC_MAX_RUN_S)"
                        )
                        break

                    if watcher.tare_count > tares_seen:
                        tares_seen = watcher.tare_count
                        self.client.tare()
                        tares += 1
                        print(
                            f"PLC: {self.config.plc_tag_tare} ON -> tared at "
                            f"{int(time.time_ns() / 1e6)} ms, still sending",
                            flush=True,
                        )

                    # Number the samples of each run from 0.
                    if first_counter is None:
                        first_counter = sample.counter
                    sample.counter -= first_counter
                    writer.write_data(sample)
                    sent += 1
                    self.health.update(
                        state="streaming",
                        session_id=self.config.session_id,
                        last_capture_time_ms=sample.capture_time_ms,
                        last_counter=sample.counter,
                    )
        except Exception as error:
            end_reason = f"error: {type(error).__name__}: {error}"
            raise
        finally:
            try:
                self.client.stop_session()
            except (ServiceError, OSError):
                pass
            tare_note = f"{tares} tare(s)" if tares else f"no {self.config.plc_tag_tare} signal came"
            print(
                f"PLC: run ended ({sent} samples sent, {tare_note}); reason: {end_reason}",
                flush=True,
            )

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
            # A session left over from a previous run is still active, so the
            # service skipped the tare. Request it explicitly.
            if self.config.request_tare:
                self.client.tare()
