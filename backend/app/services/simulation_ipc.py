"""
Mo-dun giao tiep IPC cho mo phong.
Dung cho giao tiep lien tien trinh giua backend Flask va script mo phong.

Co che lenh/phan hoi don gian dua tren he thong tep:
1. Flask ghi lenh vao thu muc `commands/`
2. Script mo phong poll thu muc lenh, thuc thi va ghi phan hoi vao `responses/`
3. Flask poll thu muc phan hoi de lay ket qua
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


class CommandType(str, Enum):
    """Loai lenh."""
    INTERVIEW = "interview"           # Phong van mot Agent
    BATCH_INTERVIEW = "batch_interview"  # Phong van hang loat
    CLOSE_ENV = "close_env"           # Dong moi truong


class CommandStatus(str, Enum):
    """Trang thai lenh."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class IPCCommand:
    """Lenh IPC."""
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
    """Phan hoi IPC."""
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
    IPC client cho mo phong, duoc su dung ben phia Flask.

    Dung de gui lenh toi tien trinh mo phong va cho phan hoi.
    """
    
    def __init__(self, simulation_dir: str):
        """
        Khoi tao IPC client.

        Args:
            simulation_dir: Thu muc du lieu mo phong.
        """
        self.simulation_dir = simulation_dir
        self.commands_dir = os.path.join(simulation_dir, "ipc_commands")
        self.responses_dir = os.path.join(simulation_dir, "ipc_responses")
        
        # Dam bao cac thu muc ton tai
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
        Gui lenh va cho phan hoi.

        Args:
            command_type: Loai lenh.
            args: Tham so lenh.
            timeout: Thoi gian timeout (giay).
            poll_interval: Chu ky poll (giay).

        Returns:
            IPCResponse

        Raises:
            TimeoutError: Het thoi gian cho phan hoi.
        """
        command_id = str(uuid.uuid4())
        command = IPCCommand(
            command_id=command_id,
            command_type=command_type,
            args=args
        )
        
        # Ghi tep lenh
        command_file = os.path.join(self.commands_dir, f"{command_id}.json")
        with open(command_file, 'w', encoding='utf-8') as f:
            json.dump(command.to_dict(), f, ensure_ascii=False, indent=2)
        
        logger.info(f"Da gui lenh IPC: {command_type.value}, command_id={command_id}")
        
        # Cho phan hoi
        response_file = os.path.join(self.responses_dir, f"{command_id}.json")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if os.path.exists(response_file):
                try:
                    with open(response_file, 'r', encoding='utf-8') as f:
                        response_data = json.load(f)
                    response = IPCResponse.from_dict(response_data)
                    
                    # Don dep tep lenh va tep phan hoi
                    try:
                        os.remove(command_file)
                        os.remove(response_file)
                    except OSError:
                        pass
                    
                    logger.info(f"Da nhan phan hoi IPC: command_id={command_id}, status={response.status.value}")
                    return response
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Parse phan hoi that bai: {e}")
            
            time.sleep(poll_interval)
        
        # Het gio cho
        logger.error(f"Cho phan hoi IPC bi timeout: command_id={command_id}")
        
        # Don dep tep lenh
        try:
            os.remove(command_file)
        except OSError:
            pass
        
        raise TimeoutError(f"Cho phan hoi cua lenh bi timeout ({timeout} giay)")
    
    def send_interview(
        self,
        agent_id: int,
        prompt: str,
        platform: str = None,
        timeout: float = 60.0
    ) -> IPCResponse:
        """
        Gui lenh phong van mot Agent.

        Args:
            agent_id: ID Agent.
            prompt: Cau hoi phong van.
            platform: Nen tang chi dinh, tuy chon.
                - `twitter`: chi phong van ben Twitter
                - `reddit`: chi phong van ben Reddit
                - `None`: neu mo phong song song thi phong van ca hai ben; neu mo phong mot nen tang thi phong van nen tang do
            timeout: Thoi gian timeout.

        Returns:
            IPCResponse, trong do `result` chua ket qua phong van.
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
        Gui lenh phong van hang loat.

        Args:
            interviews: Danh sach phong van, moi phan tu gom `{"agent_id": int, "prompt": str, "platform": str (tuy chon)}`.
            platform: Nen tang mac dinh, tuy chon; se bi ghi de boi `platform` cua tung muc neu co.
                - `twitter`: mac dinh chi phong van Twitter
                - `reddit`: mac dinh chi phong van Reddit
                - `None`: trong mo phong song song, moi Agent se duoc phong van tren ca hai nen tang
            timeout: Thoi gian timeout.

        Returns:
            IPCResponse, trong do `result` chua toan bo ket qua phong van.
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
        Gui lenh dong moi truong.

        Args:
            timeout: Thoi gian timeout.

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
        Kiem tra xem moi truong mo phong con song hay khong.

        Viec kiem tra duoc thuc hien thong qua tep `env_status.json`.
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
    IPC server cho mo phong, duoc su dung phia script mo phong.

    Server poll thu muc lenh, thuc thi lenh va tra phan hoi.
    """
    
    def __init__(self, simulation_dir: str):
        """
        Khoi tao IPC server.

        Args:
            simulation_dir: Thu muc du lieu mo phong.
        """
        self.simulation_dir = simulation_dir
        self.commands_dir = os.path.join(simulation_dir, "ipc_commands")
        self.responses_dir = os.path.join(simulation_dir, "ipc_responses")
        
        # Dam bao cac thu muc ton tai
        os.makedirs(self.commands_dir, exist_ok=True)
        os.makedirs(self.responses_dir, exist_ok=True)
        
        # Trang thai moi truong
        self._running = False
    
    def start(self):
        """Danh dau server dang o trang thai chay."""
        self._running = True
        self._update_env_status("alive")
    
    def stop(self):
        """Danh dau server dang o trang thai dung."""
        self._running = False
        self._update_env_status("stopped")
    
    def _update_env_status(self, status: str):
        """Cap nhat tep trang thai moi truong."""
        status_file = os.path.join(self.simulation_dir, "env_status.json")
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump({
                "status": status,
                "timestamp": datetime.now().isoformat()
            }, f, ensure_ascii=False, indent=2)
    
    def poll_commands(self) -> Optional[IPCCommand]:
        """
        Poll thu muc lenh va tra ve lenh dang cho dau tien.

        Returns:
            `IPCCommand` hoac `None`.
        """
        if not os.path.exists(self.commands_dir):
            return None
        
        # Lay tep lenh va sap xep theo thoi gian
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
            except (json.JSONDecodeError, KeyError, OSError) as e:
                logger.warning(f"Doc tep lenh that bai: {filepath}, {e}")
                continue
        
        return None
    
    def send_response(self, response: IPCResponse):
        """
        Gui phan hoi.

        Args:
            response: Phan hoi IPC.
        """
        response_file = os.path.join(self.responses_dir, f"{response.command_id}.json")
        with open(response_file, 'w', encoding='utf-8') as f:
            json.dump(response.to_dict(), f, ensure_ascii=False, indent=2)
        
        # Xoa tep lenh
        command_file = os.path.join(self.commands_dir, f"{response.command_id}.json")
        try:
            os.remove(command_file)
        except OSError:
            pass
    
    def send_success(self, command_id: str, result: Dict[str, Any]):
        """Gui phan hoi thanh cong."""
        self.send_response(IPCResponse(
            command_id=command_id,
            status=CommandStatus.COMPLETED,
            result=result
        ))
    
    def send_error(self, command_id: str, error: str):
        """Gui phan hoi loi."""
        self.send_response(IPCResponse(
            command_id=command_id,
            status=CommandStatus.FAILED,
            error=error
        ))
