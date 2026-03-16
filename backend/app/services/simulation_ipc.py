"""
Mô-đun giao tiếp IPC cho mô phỏng.
Dùng cho giao tiếp liên tiến trình giữa backend Flask và script mô phỏng.

Cơ chế lệnh/phản hồi đơn giản dựa trên hệ thống tệp:
1. Flask ghi lệnh vào thư mục `commands/`
2. Script mô phỏng poll thư mục lệnh, thực thi và ghi phản hồi vào `responses/`
3. Flask poll thư mục phản hồi để lấy kết quả
"""

import os
import json
import time
import uuid
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from ..utils.logger import get_logger

logger = get_logger('mirofish.simulation_ipc')


def _write_json_atomic(path: str, data: Dict[str, Any]):
    """Ghi JSON theo cách atomic để tránh đọc phải file đang ghi dở."""
    temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
    with open(temp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, path)


class CommandType(str, Enum):
    """Loại lệnh."""
    INTERVIEW = "interview"           # Phỏng vấn một Agent
    BATCH_INTERVIEW = "batch_interview"  # Phỏng vấn hàng loạt
    CLOSE_ENV = "close_env"           # Đóng môi trường


class CommandStatus(str, Enum):
    """Trạng thái lệnh."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class IPCCommand:
    """Lệnh IPC."""
    command_id: str
    command_type: CommandType
    args: Dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "command_id": self.command_id,
            "command_type": self.command_type.value,
            "args": self.args,
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IPCCommand':
        return cls(
            command_id=data["command_id"],
            command_type=CommandType(data["command_type"]),
            args=data.get("args", {}),
            timestamp=data.get("timestamp", datetime.now().isoformat())
        )


@dataclass
class IPCResponse:
    """Phản hồi IPC."""
    command_id: str
    status: CommandStatus
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "command_id": self.command_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IPCResponse':
        return cls(
            command_id=data["command_id"],
            status=CommandStatus(data["status"]),
            result=data.get("result"),
            error=data.get("error"),
            timestamp=data.get("timestamp", datetime.now().isoformat())
        )


class SimulationIPCClient:
    """
    IPC client cho mô phỏng, được sử dụng bên phía Flask.

    Dùng để gửi lệnh tới tiến trình mô phỏng và chờ phản hồi.
    """
    
    def __init__(self, simulation_dir: str):
        """
        Khởi tạo IPC client.

        Args:
            simulation_dir: Thư mục dữ liệu mô phỏng.
        """
        self.simulation_dir = simulation_dir
        self.commands_dir = os.path.join(simulation_dir, "ipc_commands")
        self.responses_dir = os.path.join(simulation_dir, "ipc_responses")
        
        # Đảm bảo các thư mục tồn tại
        os.makedirs(self.commands_dir, exist_ok=True)
        os.makedirs(self.responses_dir, exist_ok=True)
    
    def send_command(
        self,
        command_type: CommandType,
        args: Dict[str, Any],
        timeout: float = 60.0,
        poll_interval: float = 0.5
    ) -> IPCResponse:
        """
        Gửi lệnh và chờ phản hồi.

        Args:
            command_type: Loại lệnh.
            args: Tham số lệnh.
            timeout: Thời gian timeout (giây).
            poll_interval: Chu kỳ poll (giây).

        Returns:
            IPCResponse

        Raises:
            TimeoutError: Hết thời gian chờ phản hồi.
        """
        command_id = str(uuid.uuid4())
        command = IPCCommand(
            command_id=command_id,
            command_type=command_type,
            args=args
        )
        
        # Ghi tệp lệnh
        command_file = os.path.join(self.commands_dir, f"{command_id}.json")
        _write_json_atomic(command_file, command.to_dict())
        
        logger.info(f"Đã gửi lệnh IPC: {command_type.value}, command_id={command_id}")
        
        # Chờ phản hồi
        response_file = os.path.join(self.responses_dir, f"{command_id}.json")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if os.path.exists(response_file):
                try:
                    with open(response_file, 'r', encoding='utf-8') as f:
                        response_data = json.load(f)
                    response = IPCResponse.from_dict(response_data)
                    
                    # Dọn dẹp tệp lệnh và tệp phản hồi
                    try:
                        os.remove(command_file)
                        os.remove(response_file)
                    except OSError:
                        pass
                    
                    logger.info(f"Đã nhận phản hồi IPC: command_id={command_id}, status={response.status.value}")
                    return response
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    logger.warning(f"Parse phản hồi thất bại: {e}")
            
            time.sleep(poll_interval)
        
        # Hết giờ chờ
        logger.error(f"Chờ phản hồi IPC bị timeout: command_id={command_id}")
        
        # Dọn dẹp tệp lệnh
        try:
            os.remove(command_file)
        except OSError:
            pass
        
        raise TimeoutError(f"Chờ phản hồi của lệnh bị timeout ({timeout} giây)")
    
    def send_interview(
        self,
        agent_id: int,
        prompt: str,
        platform: str = None,
        timeout: float = 60.0
    ) -> IPCResponse:
        """
        Gửi lệnh phỏng vấn một Agent.

        Args:
            agent_id: ID Agent.
            prompt: Câu hỏi phỏng vấn.
            platform: Nền tảng chỉ định, tùy chọn.
                - `twitter`: chỉ phỏng vấn bên Twitter
                - `reddit`: chỉ phỏng vấn bên Reddit
                - `None`: nếu mô phỏng song song thì phỏng vấn cả hai bên; nếu mô phỏng một nền tảng thì phỏng vấn nền tảng đó
            timeout: Thời gian timeout.

        Returns:
            IPCResponse, trong đó `result` chứa kết quả phỏng vấn.
        """
        args = {
            "agent_id": agent_id,
            "prompt": prompt
        }
        if platform:
            args["platform"] = platform
            
        return self.send_command(
            command_type=CommandType.INTERVIEW,
            args=args,
            timeout=timeout
        )
    
    def send_batch_interview(
        self,
        interviews: List[Dict[str, Any]],
        platform: str = None,
        timeout: float = 120.0
    ) -> IPCResponse:
        """
        Gửi lệnh phỏng vấn hàng loạt.

        Args:
            interviews: Danh sách phỏng vấn, mỗi phần tử gồm `{"agent_id": int, "prompt": str, "platform": str (tùy chọn)}`.
            platform: Nền tảng mặc định, tùy chọn; sẽ bị ghi đè bởi `platform` của từng mục nếu có.
                - `twitter`: mặc định chỉ phỏng vấn Twitter
                - `reddit`: mặc định chỉ phỏng vấn Reddit
                - `None`: trong mô phỏng song song, mỗi Agent sẽ được phỏng vấn trên cả hai nền tảng
            timeout: Thời gian timeout.

        Returns:
            IPCResponse, trong đó `result` chứa toàn bộ kết quả phỏng vấn.
        """
        args = {"interviews": interviews}
        if platform:
            args["platform"] = platform
            
        return self.send_command(
            command_type=CommandType.BATCH_INTERVIEW,
            args=args,
            timeout=timeout
        )
    
    def send_close_env(self, timeout: float = 30.0) -> IPCResponse:
        """
        Gửi lệnh đóng môi trường.

        Args:
            timeout: Thời gian timeout.

        Returns:
            IPCResponse
        """
        return self.send_command(
            command_type=CommandType.CLOSE_ENV,
            args={},
            timeout=timeout
        )
    
    def check_env_alive(self) -> bool:
        """
        Kiểm tra xem môi trường mô phỏng còn hoạt động hay không.

        Việc kiểm tra được thực hiện thông qua tệp `env_status.json`.
        """
        status_file = os.path.join(self.simulation_dir, "env_status.json")
        if not os.path.exists(status_file):
            return False
        
        try:
            with open(status_file, 'r', encoding='utf-8') as f:
                status = json.load(f)
            return status.get("status") == "alive"
        except (json.JSONDecodeError, OSError):
            return False


class SimulationIPCServer:
    """
    IPC server cho mô phỏng, được sử dụng phía script mô phỏng.

    Server poll thư mục lệnh, thực thi lệnh và trả phản hồi.
    """
    
    def __init__(self, simulation_dir: str):
        """
        Khởi tạo IPC server.

        Args:
            simulation_dir: Thư mục dữ liệu mô phỏng.
        """
        self.simulation_dir = simulation_dir
        self.commands_dir = os.path.join(simulation_dir, "ipc_commands")
        self.responses_dir = os.path.join(simulation_dir, "ipc_responses")
        
        # Đảm bảo các thư mục tồn tại
        os.makedirs(self.commands_dir, exist_ok=True)
        os.makedirs(self.responses_dir, exist_ok=True)
        
        # Trạng thái môi trường
        self._running = False
    
    def start(self):
        """Đánh dấu server đang ở trạng thái chạy."""
        self._running = True
        self._update_env_status("alive")
    
    def stop(self):
        """Đánh dấu server đang ở trạng thái dừng."""
        self._running = False
        self._update_env_status("stopped")
    
    def _update_env_status(self, status: str):
        """Cập nhật tệp trạng thái môi trường."""
        status_file = os.path.join(self.simulation_dir, "env_status.json")
        _write_json_atomic(status_file, {
            "status": status,
            "timestamp": datetime.now().isoformat()
        })
    
    def poll_commands(self) -> Optional[IPCCommand]:
        """
        Poll thư mục lệnh và trả về lệnh đang chờ đầu tiên.

        Returns:
            `IPCCommand` hoặc `None`.
        """
        if not os.path.exists(self.commands_dir):
            return None
        
        # Lấy tệp lệnh và sắp xếp theo thời gian
        command_files = []
        for filename in os.listdir(self.commands_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(self.commands_dir, filename)
                command_files.append((filepath, os.path.getmtime(filepath)))
        
        command_files.sort(key=lambda x: x[1])
        
        for filepath, _ in command_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return IPCCommand.from_dict(data)
            except (json.JSONDecodeError, KeyError, OSError, ValueError) as e:
                logger.warning(f"Đọc tệp lệnh thất bại: {filepath}, {e}")
                continue
        
        return None
    
    def send_response(self, response: IPCResponse):
        """
        Gửi phản hồi.

        Args:
            response: Phản hồi IPC.
        """
        response_file = os.path.join(self.responses_dir, f"{response.command_id}.json")
        _write_json_atomic(response_file, response.to_dict())
        
        # Xóa tệp lệnh
        command_file = os.path.join(self.commands_dir, f"{response.command_id}.json")
        try:
            os.remove(command_file)
        except OSError:
            pass
    
    def send_success(self, command_id: str, result: Dict[str, Any]):
        """Gửi phản hồi thành công."""
        self.send_response(IPCResponse(
            command_id=command_id,
            status=CommandStatus.COMPLETED,
            result=result
        ))
    
    def send_error(self, command_id: str, error: str):
        """Gửi phản hồi lỗi."""
        self.send_response(IPCResponse(
            command_id=command_id,
            status=CommandStatus.FAILED,
            error=error
        ))
