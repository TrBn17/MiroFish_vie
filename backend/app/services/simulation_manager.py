"""
Bộ quản lý mô phỏng OASIS.
Quản lý mô phỏng song song trên Twitter và Reddit.
Sử dụng script có sẵn kết hợp với LLM để tạo tham số cấu hình.
"""

import os
import json
import shutil
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from ..config import Config
from ..utils.logger import get_logger
from .zep_entity_reader import ZepEntityReader, FilteredEntities
from .oasis_profile_generator import OasisProfileGenerator, OasisAgentProfile, normalize_interested_topics
from .simulation_config_generator import SimulationConfigGenerator, SimulationParameters

logger = get_logger('mirofish.simulation')


def _normalize_profile_topics(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize topic fields from persisted profile data."""
    normalized = dict(profile)
    normalized["interested_topics"] = normalize_interested_topics(profile.get("interested_topics"))
    return normalized


class SimulationStatus(str, Enum):
    """Trạng thái mô phỏng."""
    CREATED = "created"
    PREPARING = "preparing"
    READY = "ready"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"      # Mô phỏng bị dừng thủ công
    COMPLETED = "completed"  # Mô phỏng kết thúc tự nhiên
    FAILED = "failed"


class PlatformType(str, Enum):
    """Loại nền tảng."""
    TWITTER = "twitter"
    REDDIT = "reddit"


@dataclass
class SimulationState:
    """Trạng thái mô phỏng."""
    simulation_id: str
    project_id: str
    graph_id: str
    
    # Trạng thái bật nền tảng
    enable_twitter: bool = True
    enable_reddit: bool = True
    
    # Trạng thái chung
    status: SimulationStatus = SimulationStatus.CREATED
    
    # Dữ liệu giai đoạn chuẩn bị
    entities_count: int = 0
    profiles_count: int = 0
    entity_types: List[str] = field(default_factory=list)
    
    # Thông tin tạo cấu hình
    config_generated: bool = False
    config_reasoning: str = ""
    
    # Dữ liệu trong lúc chạy
    current_round: int = 0
    twitter_status: str = "not_started"
    reddit_status: str = "not_started"
    
    # Mốc thời gian
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # Thông tin lỗi
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Trả về dict đầy đủ dùng nội bộ."""
        return {
            "simulation_id": self.simulation_id,
            "project_id": self.project_id,
            "graph_id": self.graph_id,
            "enable_twitter": self.enable_twitter,
            "enable_reddit": self.enable_reddit,
            "status": self.status.value,
            "entities_count": self.entities_count,
            "profiles_count": self.profiles_count,
            "entity_types": self.entity_types,
            "config_generated": self.config_generated,
            "config_reasoning": self.config_reasoning,
            "current_round": self.current_round,
            "twitter_status": self.twitter_status,
            "reddit_status": self.reddit_status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "error": self.error,
        }
    
    def to_simple_dict(self) -> Dict[str, Any]:
        """Trả về dict rút gọn dùng cho API."""
        return {
            "simulation_id": self.simulation_id,
            "project_id": self.project_id,
            "graph_id": self.graph_id,
            "status": self.status.value,
            "entities_count": self.entities_count,
            "profiles_count": self.profiles_count,
            "entity_types": self.entity_types,
            "config_generated": self.config_generated,
            "error": self.error,
        }


class SimulationManager:
    """
    Bộ quản lý mô phỏng.

    Chức năng chính:
    1. Đọc và lọc thực thể từ đồ thị Zep.
    2. Tạo OASIS Agent Profile.
    3. Dùng LLM để tạo tham số cấu hình mô phỏng.
    4. Chuẩn bị đầy đủ các tệp cần cho script có sẵn.
    """
    
    # Thư mục lưu dữ liệu mô phỏng
    SIMULATION_DATA_DIR = os.path.join(
        os.path.dirname(__file__), 
        '../../uploads/simulations'
    )
    
    def __init__(self):
        # Đảm bảo thư mục tồn tại
        os.makedirs(self.SIMULATION_DATA_DIR, exist_ok=True)
        
        # Bộ đệm trạng thái mô phỏng trong bộ nhớ
        self._simulations: Dict[str, SimulationState] = {}
    
    def _get_simulation_dir(self, simulation_id: str) -> str:
        """Lấy thư mục dữ liệu của mô phỏng."""
        sim_dir = os.path.join(self.SIMULATION_DATA_DIR, simulation_id)
        os.makedirs(sim_dir, exist_ok=True)
        return sim_dir
    
    def _save_simulation_state(self, state: SimulationState):
        """Lưu trạng thái mô phỏng ra tệp."""
        sim_dir = self._get_simulation_dir(state.simulation_id)
        state_file = os.path.join(sim_dir, "state.json")
        
        state.updated_at = datetime.now().isoformat()
        
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state.to_dict(), f, ensure_ascii=False, indent=2)
        
        self._simulations[state.simulation_id] = state
    
    def _load_simulation_state(self, simulation_id: str) -> Optional[SimulationState]:
        """Nạp trạng thái mô phỏng từ tệp."""
        if simulation_id in self._simulations:
            return self._simulations[simulation_id]
        
        sim_dir = self._get_simulation_dir(simulation_id)
        state_file = os.path.join(sim_dir, "state.json")
        
        if not os.path.exists(state_file):
            return None
        
        with open(state_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        state = SimulationState(
            simulation_id=simulation_id,
            project_id=data.get("project_id", ""),
            graph_id=data.get("graph_id", ""),
            enable_twitter=data.get("enable_twitter", True),
            enable_reddit=data.get("enable_reddit", True),
            status=SimulationStatus(data.get("status", "created")),
            entities_count=data.get("entities_count", 0),
            profiles_count=data.get("profiles_count", 0),
            entity_types=data.get("entity_types", []),
            config_generated=data.get("config_generated", False),
            config_reasoning=data.get("config_reasoning", ""),
            current_round=data.get("current_round", 0),
            twitter_status=data.get("twitter_status", "not_started"),
            reddit_status=data.get("reddit_status", "not_started"),
            created_at=data.get("created_at", datetime.now().isoformat()),
            updated_at=data.get("updated_at", datetime.now().isoformat()),
            error=data.get("error"),
        )
        
        self._simulations[simulation_id] = state
        return state
    
    def create_simulation(
        self,
        project_id: str,
        graph_id: str,
        enable_twitter: bool = True,
        enable_reddit: bool = True,
    ) -> SimulationState:
        """
        Tạo mô phỏng mới.

        Args:
            project_id: ID dự án.
            graph_id: ID đồ thị Zep.
            enable_twitter: Có bật mô phỏng Twitter hay không.
            enable_reddit: Có bật mô phỏng Reddit hay không.

        Returns:
            SimulationState
        """
        import uuid
        simulation_id = f"sim_{uuid.uuid4().hex[:12]}"
        
        state = SimulationState(
            simulation_id=simulation_id,
            project_id=project_id,
            graph_id=graph_id,
            enable_twitter=enable_twitter,
            enable_reddit=enable_reddit,
            status=SimulationStatus.CREATED,
        )
        
        self._save_simulation_state(state)
        logger.info(f"Tạo mô phỏng: {simulation_id}, project={project_id}, graph={graph_id}")
        
        return state
    
    def prepare_simulation(
        self,
        simulation_id: str,
        simulation_requirement: str,
        document_text: str,
        defined_entity_types: Optional[List[str]] = None,
        use_llm_for_profiles: bool = True,
        progress_callback: Optional[callable] = None,
        parallel_profile_count: int = 3
    ) -> SimulationState:
        """
        Chuẩn bị môi trường mô phỏng theo quy trình tự động.

        Các bước:
        1. Đọc và lọc thực thể từ đồ thị Zep.
        2. Tạo OASIS Agent Profile cho từng thực thể (có thể tăng cường bằng LLM, hỗ trợ song song).
        3. Dùng LLM để tạo tham số cấu hình mô phỏng (thời gian, mức độ hoạt động, tần suất phát ngôn...).
        4. Lưu tệp cấu hình và tệp profile.
        5. Chuẩn bị các script cần thiết cho việc chạy mô phỏng.

        Args:
            simulation_id: ID mô phỏng.
            simulation_requirement: Mô tả nhu cầu mô phỏng để LLM tạo cấu hình.
            document_text: Nội dung văn bản gốc để LLM hiểu bối cảnh.
            defined_entity_types: Các loại thực thể định nghĩa trước (tùy chọn).
            use_llm_for_profiles: Có dùng LLM để tạo persona chi tiết hay không.
            progress_callback: Hàm callback tiến độ `(stage, progress, message)`.
            parallel_profile_count: Số lượng persona được tạo song song, mặc định 3.

        Returns:
            SimulationState
        """
        state = self._load_simulation_state(simulation_id)
        if not state:
            raise ValueError(f"Không tìm thấy mô phỏng: {simulation_id}")
        
        try:
            state.status = SimulationStatus.PREPARING
            self._save_simulation_state(state)
            
            sim_dir = self._get_simulation_dir(simulation_id)
            
            # ========== Giai đoạn 1: đọc và lọc thực thể ==========
            if progress_callback:
                progress_callback("reading", 0, "Đang kết nối tới đồ thị Zep...")
            
            reader = ZepEntityReader()
            
            if progress_callback:
                progress_callback("reading", 30, "Đang đọc dữ liệu node...")
            
            filtered = reader.filter_defined_entities(
                graph_id=state.graph_id,
                defined_entity_types=defined_entity_types,
                enrich_with_edges=True
            )
            
            state.entities_count = filtered.filtered_count
            state.entity_types = list(filtered.entity_types)
            
            if progress_callback:
                progress_callback(
                    "reading", 100, 
                    f"Hoàn tất, có {filtered.filtered_count} thực thể",
                    current=filtered.filtered_count,
                    total=filtered.filtered_count
                )
            
            if filtered.filtered_count == 0:
                state.status = SimulationStatus.FAILED
                state.error = "Không tìm thấy thực thể phù hợp. Hãy kiểm tra lại quá trình xây dựng đồ thị"
                self._save_simulation_state(state)
                return state
            
            # ========== Giai đoạn 2: tạo Agent Profile ==========
            total_entities = len(filtered.entities)
            
            if progress_callback:
                progress_callback(
                    "generating_profiles", 0, 
                    "Bắt đầu tạo...",
                    current=0,
                    total=total_entities
                )
            
            # Truyền graph_id để bật truy xuất Zep và lấy thêm ngữ cảnh
            generator = OasisProfileGenerator(graph_id=state.graph_id)
            
            def profile_progress(current, total, msg):
                if progress_callback:
                    progress_callback(
                        "generating_profiles", 
                        int(current / total * 100), 
                        msg,
                        current=current,
                        total=total,
                        item_name=msg
                    )
            
            # Thiết lập đường dẫn lưu theo thời gian thực, ưu tiên định dạng Reddit JSON
            realtime_output_path = None
            realtime_platform = "reddit"
            if state.enable_reddit:
                realtime_output_path = os.path.join(sim_dir, "reddit_profiles.json")
                realtime_platform = "reddit"
            elif state.enable_twitter:
                realtime_output_path = os.path.join(sim_dir, "twitter_profiles.csv")
                realtime_platform = "twitter"
            
            profiles = generator.generate_profiles_from_entities(
                entities=filtered.entities,
                use_llm=use_llm_for_profiles,
                progress_callback=profile_progress,
                graph_id=state.graph_id,  # Truyền graph_id để truy xuất Zep
                parallel_count=parallel_profile_count,  # Số lượng tác vụ chạy song song
                realtime_output_path=realtime_output_path,  # Đường dẫn lưu theo thời gian thực
                output_platform=realtime_platform  # Định dạng đầu ra
            )
            
            state.profiles_count = len(profiles)
            
            # Lưu tệp Profile (Twitter dùng CSV, Reddit dùng JSON)
            # Reddit đã được lưu trong lúc tạo, ở đây lưu lại một lần nữa để đảm bảo đầy đủ
            if progress_callback:
                progress_callback(
                    "generating_profiles", 95, 
                    "Đang lưu tệp Profile...",
                    current=total_entities,
                    total=total_entities
                )
            
            if state.enable_reddit:
                generator.save_profiles(
                    profiles=profiles,
                    file_path=os.path.join(sim_dir, "reddit_profiles.json"),
                    platform="reddit"
                )
            
            if state.enable_twitter:
                # Twitter bắt buộc dùng định dạng CSV theo yêu cầu của OASIS
                generator.save_profiles(
                    profiles=profiles,
                    file_path=os.path.join(sim_dir, "twitter_profiles.csv"),
                    platform="twitter"
                )
            
            if progress_callback:
                progress_callback(
                    "generating_profiles", 100, 
                    f"Hoàn tất, tạo được {len(profiles)} Profile",
                    current=len(profiles),
                    total=len(profiles)
                )
            
            # ========== Giai đoạn 3: dùng LLM để tạo cấu hình mô phỏng ==========
            if progress_callback:
                progress_callback(
                    "generating_config", 0, 
                    "Đang phân tích nhu cầu mô phỏng...",
                    current=0,
                    total=3
                )
            
            config_generator = SimulationConfigGenerator()
            
            if progress_callback:
                progress_callback(
                    "generating_config", 30, 
                    "Đang gọi LLM để tạo cấu hình...",
                    current=1,
                    total=3
                )
            
            sim_params = config_generator.generate_config(
                simulation_id=simulation_id,
                project_id=state.project_id,
                graph_id=state.graph_id,
                simulation_requirement=simulation_requirement,
                document_text=document_text,
                entities=filtered.entities,
                enable_twitter=state.enable_twitter,
                enable_reddit=state.enable_reddit
            )
            
            if progress_callback:
                progress_callback(
                    "generating_config", 70, 
                    "Đang lưu tệp cấu hình...",
                    current=2,
                    total=3
                )
            
            # Lưu tệp cấu hình
            config_path = os.path.join(sim_dir, "simulation_config.json")
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(sim_params.to_json())
            
            state.config_generated = True
            state.config_reasoning = sim_params.generation_reasoning
            
            if progress_callback:
                progress_callback(
                    "generating_config", 100, 
                    "Đã tạo xong cấu hình",
                    current=3,
                    total=3
                )
            
            # Lưu ý: script chạy vẫn nằm ở `backend/scripts/`, không copy vào thư mục mô phỏng nữa
            # Khi khởi động mô phỏng, `simulation_runner` sẽ chạy trực tiếp script từ thư mục `scripts/`
            
            # Cập nhật trạng thái
            state.status = SimulationStatus.READY
            self._save_simulation_state(state)
            
            logger.info(f"Chuẩn bị mô phỏng hoàn tất: {simulation_id}, "
                       f"entities={state.entities_count}, profiles={state.profiles_count}")
            
            return state
            
        except Exception as e:
            logger.error(f"Chuẩn bị mô phỏng thất bại: {simulation_id}, error={str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            state.status = SimulationStatus.FAILED
            state.error = str(e)
            self._save_simulation_state(state)
            raise
    
    def get_simulation(self, simulation_id: str) -> Optional[SimulationState]:
        """Lấy trạng thái mô phỏng."""
        return self._load_simulation_state(simulation_id)
    
    def list_simulations(self, project_id: Optional[str] = None) -> List[SimulationState]:
        """Liệt kê toàn bộ mô phỏng."""
        simulations = []
        
        if os.path.exists(self.SIMULATION_DATA_DIR):
            for sim_id in os.listdir(self.SIMULATION_DATA_DIR):
                # Bỏ qua tệp ẩn (ví dụ `.DS_Store`) và mục không phải thư mục
                sim_path = os.path.join(self.SIMULATION_DATA_DIR, sim_id)
                if sim_id.startswith('.') or not os.path.isdir(sim_path):
                    continue
                
                state = self._load_simulation_state(sim_id)
                if state:
                    if project_id is None or state.project_id == project_id:
                        simulations.append(state)
        
        return simulations
    
    def get_profiles(self, simulation_id: str, platform: str = "reddit") -> List[Dict[str, Any]]:
        """Lấy Agent Profile của mô phỏng."""
        state = self._load_simulation_state(simulation_id)
        if not state:
            raise ValueError(f"Không tìm thấy mô phỏng: {simulation_id}")
        
        sim_dir = self._get_simulation_dir(simulation_id)
        if platform == "reddit":
            profile_path = os.path.join(sim_dir, "reddit_profiles.json")
        else:
            profile_path = os.path.join(sim_dir, "twitter_profiles.csv")
        
        if not os.path.exists(profile_path):
            return []
        
        if platform == "reddit":
            with open(profile_path, 'r', encoding='utf-8') as f:
                profiles = json.load(f)
        else:
            import csv

            with open(profile_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                profiles = list(reader)

        return [
            _normalize_profile_topics(profile)
            for profile in profiles
            if isinstance(profile, dict)
        ]
    
    def get_simulation_config(self, simulation_id: str) -> Optional[Dict[str, Any]]:
        """Lấy cấu hình mô phỏng."""
        sim_dir = self._get_simulation_dir(simulation_id)
        config_path = os.path.join(sim_dir, "simulation_config.json")
        
        if not os.path.exists(config_path):
            return None
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_run_instructions(self, simulation_id: str) -> Dict[str, str]:
        """Lấy hướng dẫn chạy mô phỏng."""
        sim_dir = self._get_simulation_dir(simulation_id)
        config_path = os.path.join(sim_dir, "simulation_config.json")
        scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
        
        return {
            "simulation_dir": sim_dir,
            "scripts_dir": scripts_dir,
            "config_file": config_path,
            "commands": {
                "twitter": f"python {scripts_dir}/run_twitter_simulation.py --config {config_path}",
                "reddit": f"python {scripts_dir}/run_reddit_simulation.py --config {config_path}",
                "parallel": f"python {scripts_dir}/run_parallel_simulation.py --config {config_path}",
            },
            "instructions": (
                f"1. Kích hoạt môi trường conda: conda activate MiroFish\n"
                f"2. Chạy mô phỏng (script nằm tại {scripts_dir}):\n"
                f"   - Chạy riêng Twitter: python {scripts_dir}/run_twitter_simulation.py --config {config_path}\n"
                f"   - Chạy riêng Reddit: python {scripts_dir}/run_reddit_simulation.py --config {config_path}\n"
                f"   - Chạy song song cả hai nền tảng: python {scripts_dir}/run_parallel_simulation.py --config {config_path}"
            )
        }
