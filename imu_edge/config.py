from dataclasses import dataclass
import os
import socket
import time


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class EdgeAgentConfig:
    socket_path: str
    session_id: str
    sample_hz: int
    request_tare: bool
    reset_counter: bool
    reconnect_delay_s: float
    mqtt_broker_ip: str
    mqtt_broker_port: int
    device_id: int
    csv_path: str | None
    health_path: str
    stale_after_s: int
    start_session_on_boot: bool
    stop_session_on_exit: bool
    wait_for_trigger: bool  #this is the addition by umar khatab for the udp system
    trigger_port: int
    trigger_payload:str
    trigger_settle_s:float
    @classmethod
    def from_env(cls) -> "EdgeAgentConfig":
        hostname = socket.gethostname()
        timestamp = int(time.time())
        csv_path = os.getenv("IMU_CSV_PATH")
        return cls(
            socket_path=os.getenv("IMU_SOCKET_PATH", "/run/imu-hw/imu.sock"),
            session_id=os.getenv("IMU_SESSION_ID", f"{hostname}-{timestamp}"),
            sample_hz=max(1, int(os.getenv("IMU_SAMPLE_HZ", "100"))),
            request_tare=_env_flag("IMU_REQUEST_TARE", False),
            reset_counter=_env_flag("IMU_RESET_COUNTER", True),
            reconnect_delay_s=float(os.getenv("IMU_RECONNECT_DELAY_S", "2.0")),
            mqtt_broker_ip=os.getenv("MQTT_BROKER_IP", "127.0.0.1"),
            mqtt_broker_port=int(os.getenv("MQTT_BROKER_PORT", "1883")),
            device_id=int(os.getenv("DEVICE_ID", "0")),
            csv_path=csv_path if csv_path else None,
            health_path=os.getenv("IMU_EDGE_HEALTH_PATH", "/tmp/imu-edge-health.json"),
            stale_after_s=max(5, int(os.getenv("IMU_EDGE_STALE_AFTER_S", "30"))),
            start_session_on_boot=_env_flag("IMU_AUTO_START_SESSION", True),
            stop_session_on_exit=_env_flag("IMU_STOP_SESSION_ON_EXIT", False),
	    wait_for_trigger=_env_flag("IMU_WAIT_FOR_UDP_TRIGGER", False), #this is the addition by umar khattab for the udp system and below
            trigger_port=int(os.getenv("IMU_TRIGGER_UDP_PORT", "5005")),
            trigger_payload=os.getenv("IMU_TRIGGER_PAYLOAD", "1"),
            trigger_settle_s=float(os.getenv("IMU_TRIGGER_SETTLE_S", "1.0")),
        
)
