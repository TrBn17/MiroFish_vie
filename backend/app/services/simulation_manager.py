"""
Bo quan ly mo phong OASIS.
Quan ly mo phong song song tren Twitter va Reddit.
Su dung script co san ket hop voi LLM de tao tham so cau hinh.
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
from .oasis_profile_generator import OasisProfileGenerator, OasisAgentProfile
from .simulation_config_generator import SimulationConfigGenerator, SimulationParameters

logger = get_logger('mirofish.simulation')


class SimulationStatus(str, Enum):
    """Trang thai mo phong."""
    CREATED = "created"
    PREPARING = "preparing"
    READY = "ready"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"      # Mo phong bi dung thu cong
    COMPLETED = "completed"  # Mo phong ket thuc tu nhien
    FAILED = "failed"


class PlatformType(str, Enum):
    """Loai nen tang."""
    TWITTER = "twitter"
    REDDIT = "reddit"


@dataclass
class SimulationState:
    """Trang thai mo phong."""
    simulation_id: str
    project_id: str
    graph_id: str
    
    # Trang thai bat nen tang
    enable_twitter: bool = True
    enable_reddit: bool = True
    
    # Trang thai chung
    status: SimulationStatus = SimulationStatus.CREATED
    
    # Du lieu giai doan chuan bi
    entities_count: int = 0
    profiles_count: int = 0
    entity_types: List[str] = field(default_factory=list)
    
    # Thong tin tao cau hinh
    config_generated: bool = False
    config_reasoning: str = ""
    
    # Du lieu trong luc chay
    current_round: int = 0
    twitter_status: str = "not_started"
    reddit_status: str = "not_started"
    
    # Moc thoi gian
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # Thong tin loi
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Tra ve dict day du dung noi bo."""
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
        """Tra ve dict rut gon dung cho API."""
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
    Bo quan ly mo phong.

    Chuc nang chinh:
    1. Doc va loc thuc the tu do thi Zep.
    2. Tao OASIS Agent Profile.
    3. Dung LLM de tao tham so cau hinh mo phong.
    4. Chuan bi day du cac tep can cho script co san.
    """
    
    # Thu muc luu du lieu mo phong
    SIMULATION_DATA_DIR = os.path.join(
        os.path.dirname(__file__), 
        '../../uploads/simulations'
    )
    
    def __init__(self):
        # Dam bao thu muc ton tai
        os.makedirs(self.SIMULATION_DATA_DIR, exist_ok=True)
        
        # Bo dem trang thai mo phong trong bo nho
        self._simulations: Dict[str, SimulationState] = {}
    
    def _get_simulation_dir(self, simulation_id: str) -> str:
        """Lay thu muc du lieu cua mo phong."""
        sim_dir = os.path.join(self.SIMULATION_DATA_DIR, simulation_id)
        os.makedirs(sim_dir, exist_ok=True)
        return sim_dir
    
    def _save_simulation_state(self, state: SimulationState):
        """Luu trang thai mo phong ra tep."""
        sim_dir = self._get_simulation_dir(state.simulation_id)
        state_file = os.path.join(sim_dir, "state.json")
        
        state.updated_at = datetime.now().isoformat()
        
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state.to_dict(), f, ensure_ascii=False, indent=2)
        
        self._simulations[state.simulation_id] = state
    
    def _load_simulation_state(self, simulation_id: str) -> Optional[SimulationState]:
        """Nap trang thai mo phong tu tep."""
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
        Tao mo phong moi.

        Args:
            project_id: ID du an.
            graph_id: ID do thi Zep.
            enable_twitter: Co bat mo phong Twitter hay khong.
            enable_reddit: Co bat mo phong Reddit hay khong.

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
        logger.info(f"Tao mo phong: {simulation_id}, project={project_id}, graph={graph_id}")
        
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
        Chuan bi moi truong mo phong theo quy trinh tu dong.

        Cac buoc:
        1. Doc va loc thuc the tu do thi Zep.
        2. Tao OASIS Agent Profile cho tung thuc the (co the tang cuong bang LLM, ho tro song song).
        3. Dung LLM de tao tham so cau hinh mo phong (thoi gian, muc do hoat dong, tan suat phat ngon...).
        4. Luu tep cau hinh va tep profile.
        5. Chuan bi cac script can thiet cho viec chay mo phong.

        Args:
            simulation_id: ID mo phong.
            simulation_requirement: Mo ta nhu cau mo phong de LLM tao cau hinh.
            document_text: Noi dung van ban goc de LLM hieu boi canh.
            defined_entity_types: Cac loai thuc the dinh nghia truoc (tuy chon).
            use_llm_for_profiles: Co dung LLM de tao persona chi tiet hay khong.
            progress_callback: Ham callback tien do `(stage, progress, message)`.
            parallel_profile_count: So luong persona duoc tao song song, mac dinh 3.

        Returns:
            SimulationState
        """
        state = self._load_simulation_state(simulation_id)
        if not state:
            raise ValueError(f"Khong tim thay mo phong: {simulation_id}")
        
        try:
            state.status = SimulationStatus.PREPARING
            self._save_simulation_state(state)
            
            sim_dir = self._get_simulation_dir(simulation_id)
            
            # ========== Giai doan 1: doc va loc thuc the ==========
            if progress_callback:
                progress_callback("reading", 0, "Dang ket noi toi do thi Zep...")
            
            reader = ZepEntityReader()
            
            if progress_callback:
                progress_callback("reading", 30, "Dang doc du lieu node...")
            
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
                    f"Hoan tat, co {filtered.filtered_count} thuc the",
                    current=filtered.filtered_count,
                    total=filtered.filtered_count
                )
            
            if filtered.filtered_count == 0:
                state.status = SimulationStatus.FAILED
                state.error = "Khong tim thay thuc the phu hop. Hay kiem tra lai qua trinh xay dung do thi"
                self._save_simulation_state(state)
                return state
            
            # ========== Giai doan 2: tao Agent Profile ==========
            total_entities = len(filtered.entities)
            
            if progress_callback:
                progress_callback(
                    "generating_profiles", 0, 
                    "Bat dau tao...",
                    current=0,
                    total=total_entities
                )
            
            # Truyen graph_id de bat truy xuat Zep va lay them ngu canh
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
            
            # Thiet lap duong dan luu theo thoi gian thuc, uu tien dinh dang Reddit JSON
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
                graph_id=state.graph_id,  # Truyen graph_id de truy xuat Zep
                parallel_count=parallel_profile_count,  # So luong tac vu chay song song
                realtime_output_path=realtime_output_path,  # Duong dan luu theo thoi gian thuc
                output_platform=realtime_platform  # Dinh dang dau ra
            )
            
            state.profiles_count = len(profiles)
            
            # Luu tep Profile (Twitter dung CSV, Reddit dung JSON)
            # Reddit da duoc luu trong luc tao, o day luu lai mot lan nua de dam bao day du
            if progress_callback:
                progress_callback(
                    "generating_profiles", 95, 
                    "Dang luu tep Profile...",
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
                # Twitter bat buoc dung dinh dang CSV theo yeu cau cua OASIS
                generator.save_profiles(
                    profiles=profiles,
                    file_path=os.path.join(sim_dir, "twitter_profiles.csv"),
                    platform="twitter"
                )
            
            if progress_callback:
                progress_callback(
                    "generating_profiles", 100, 
                    f"Hoan tat, tao duoc {len(profiles)} Profile",
                    current=len(profiles),
                    total=len(profiles)
                )
            
            # ========== Giai doan 3: dung LLM de tao cau hinh mo phong ==========
            if progress_callback:
                progress_callback(
                    "generating_config", 0, 
                    "Dang phan tich nhu cau mo phong...",
                    current=0,
                    total=3
                )
            
            config_generator = SimulationConfigGenerator()
            
            if progress_callback:
                progress_callback(
                    "generating_config", 30, 
                    "Dang goi LLM de tao cau hinh...",
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
                    "Dang luu tep cau hinh...",
                    current=2,
                    total=3
                )
            
            # Luu tep cau hinh
            config_path = os.path.join(sim_dir, "simulation_config.json")
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(sim_params.to_json())
            
            state.config_generated = True
            state.config_reasoning = sim_params.generation_reasoning
            
            if progress_callback:
                progress_callback(
                    "generating_config", 100, 
                    "Da tao xong cau hinh",
                    current=3,
                    total=3
                )
            
            # Luu y: script chay van nam o `backend/scripts/`, khong copy vao thu muc mo phong nua
            # Khi khoi dong mo phong, `simulation_runner` se chay truc tiep script tu thu muc `scripts/`
            
            # Cap nhat trang thai
            state.status = SimulationStatus.READY
            self._save_simulation_state(state)
            
            logger.info(f"Chuan bi mo phong hoan tat: {simulation_id}, "
                       f"entities={state.entities_count}, profiles={state.profiles_count}")
            
            return state
            
        except Exception as e:
            logger.error(f"Chuan bi mo phong that bai: {simulation_id}, error={str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            state.status = SimulationStatus.FAILED
            state.error = str(e)
            self._save_simulation_state(state)
            raise
    
    def get_simulation(self, simulation_id: str) -> Optional[SimulationState]:
        """Lay trang thai mo phong."""
        return self._load_simulation_state(simulation_id)
    
    def list_simulations(self, project_id: Optional[str] = None) -> List[SimulationState]:
        """Liet ke toan bo mo phong."""
        simulations = []
        
        if os.path.exists(self.SIMULATION_DATA_DIR):
            for sim_id in os.listdir(self.SIMULATION_DATA_DIR):
                # Bo qua tep an (vi du `.DS_Store`) va muc khong phai thu muc
                sim_path = os.path.join(self.SIMULATION_DATA_DIR, sim_id)
                if sim_id.startswith('.') or not os.path.isdir(sim_path):
                    continue
                
                state = self._load_simulation_state(sim_id)
                if state:
                    if project_id is None or state.project_id == project_id:
                        simulations.append(state)
        
        return simulations
    
    def get_profiles(self, simulation_id: str, platform: str = "reddit") -> List[Dict[str, Any]]:
        """Lay Agent Profile cua mo phong."""
        state = self._load_simulation_state(simulation_id)
        if not state:
            raise ValueError(f"Khong tim thay mo phong: {simulation_id}")
        
        sim_dir = self._get_simulation_dir(simulation_id)
        profile_path = os.path.join(sim_dir, f"{platform}_profiles.json")
        
        if not os.path.exists(profile_path):
            return []
        
        with open(profile_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_simulation_config(self, simulation_id: str) -> Optional[Dict[str, Any]]:
        """Lay cau hinh mo phong."""
        sim_dir = self._get_simulation_dir(simulation_id)
        config_path = os.path.join(sim_dir, "simulation_config.json")
        
        if not os.path.exists(config_path):
            return None
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_run_instructions(self, simulation_id: str) -> Dict[str, str]:
        """Lay huong dan chay mo phong."""
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
                f"1. Kich hoat moi truong conda: conda activate MiroFish\n"
                f"2. Chay mo phong (script nam tai {scripts_dir}):\n"
                f"   - Chay rieng Twitter: python {scripts_dir}/run_twitter_simulation.py --config {config_path}\n"
                f"   - Chay rieng Reddit: python {scripts_dir}/run_reddit_simulation.py --config {config_path}\n"
                f"   - Chay song song ca hai nen tang: python {scripts_dir}/run_parallel_simulation.py --config {config_path}"
            )
        }
