"""
Watch the robot cell's PLC for the signals that drive a capture run.

    Data_Start ON   -> start capturing IMU data (not sending yet)
    IMU_Tare ON     -> tare once, then start sending
    Data_Start OFF  -> stop        (Db_Stop ON also stops)

The watcher polls the tags in a background thread so the agent can keep
reading IMU samples while it waits for a signal.
"""
import threading
import time
from collections import namedtuple
from typing import Callable

FakeTag = namedtuple("FakeTag", "tag value type error")


class FakePLC:
    """
    Stand-in for pycomm3's LogixDriver, for testing without the robot.
    Plays back the signal sequence of the robot program SJOINT1.
    """

    DEFAULT_TIMELINE = [
        (3.0, "Data_Start", True),    # DO[29:RPi_START]=ON
        (10.0, "IMU_Tare", True),     # DO[30:TARE_START]=ON  (after the jerk)
        (11.0, "IMU_Tare", False),    # 1 s later
        (25.0, "Data_Start", False),  # DO[29:RPi_START]=OFF
    ]

    def __init__(self, timeline=None, speed: float = 1.0, **_ignored):
        self.timeline = sorted(timeline or self.DEFAULT_TIMELINE)
        self.speed = speed
        self._t0 = time.monotonic()

    def __enter__(self):
        self._t0 = time.monotonic()
        return self

    def __exit__(self, *exc):
        return False

    def read(self, *tags):
        elapsed = (time.monotonic() - self._t0) * self.speed
        state = {tag: False for _at, tag, _value in self.timeline}
        for at, tag, value in self.timeline:
            if elapsed >= at:
                state[tag] = value

        results = [
            FakeTag(tag, state[tag], "BOOL", None)
            if tag in state
            else FakeTag(tag, None, None, "Tag doesn't exist")
            for tag in tags
        ]
        return results[0] if len(results) == 1 else results


def build_plc_factory(path: str, fake: bool = False, fake_speed: float = 1.0) -> Callable:
    """Return a function that opens a connection to the PLC (or the stand-in)."""
    if fake:
        return lambda: FakePLC(speed=fake_speed)

    def _open_real():
        # Imported here so pycomm3 is only needed when the PLC trigger is used.
        from pycomm3 import LogixDriver

        return LogixDriver(path, init_connection=False, large_packets=False)

    return _open_real


class PLCSignalWatcher:
    def __init__(
        self,
        plc_factory: Callable,
        tag_start: str = "Data_Start",
        tag_tare: str = "IMU_Tare",
        tag_stop: str = "Db_Stop",
        poll_s: float = 0.05,
        retry_delay_s: float = 1.0,
    ):
        self._plc_factory = plc_factory
        self._tags = (tag_start, tag_tare, tag_stop)
        self._poll_s = poll_s
        self._retry_delay_s = retry_delay_s

        self._lock = threading.Lock()
        self._start = False
        self._stop = False
        self._tare_count = 0
        self._connected = False
        self._last_error: str | None = None

        self._closing = threading.Event()
        self._thread = threading.Thread(target=self._run, name="plc-signals", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._closing.set()
        self._thread.join(timeout=5)

    @property
    def run_requested(self) -> bool:
        """True while the robot wants data captured: Data_Start on and Db_Stop off."""
        with self._lock:
            return self._start and not self._stop

    @property
    def tare_count(self) -> int:
        """How many times IMU_Tare has turned on. A 1 s pulse counts once."""
        with self._lock:
            return self._tare_count

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._connected

    @property
    def last_error(self) -> str | None:
        with self._lock:
            return self._last_error

    @staticmethod
    def _as_bool(result) -> bool:
        return bool(result.value) if result.error is None else False

    def _run(self) -> None:
        last_tare = False
        while not self._closing.is_set():
            try:
                with self._plc_factory() as plc:
                    with self._lock:
                        self._connected = True
                        self._last_error = None

                    while not self._closing.is_set():
                        start_r, tare_r, stop_r = plc.read(*self._tags)
                        start = self._as_bool(start_r)
                        tare = self._as_bool(tare_r)
                        stop = self._as_bool(stop_r)
                        # A missing start or tare tag is a setup mistake worth
                        # reporting. Db_Stop is optional.
                        error = start_r.error or tare_r.error

                        with self._lock:
                            self._start = start
                            self._stop = stop
                            if tare and not last_tare:
                                self._tare_count += 1
                            self._last_error = str(error) if error else None
                        last_tare = tare

                        self._closing.wait(self._poll_s)
            except Exception as error:  # connection lost or refused: retry
                # The last known signal state is kept, so a short network
                # glitch does not cut a run in half.
                with self._lock:
                    self._connected = False
                    self._last_error = str(error)
                self._closing.wait(self._retry_delay_s)
