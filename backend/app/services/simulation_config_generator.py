"""
Simulation Configuration Smart Generator
Automatically generate detailed simulation parameters using LLM based on simulation requirements, document content, and graph information.
Fully automated, no manual parameter setting required.

Uses a step-by-step generation strategy to avoid failures caused by generating too much content at once:
1. Generate time configuration
2. Generate event configuration
3. Generate Agent configuration in batches
4. Generate platform configuration
"""

import json
import math
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime

from openai import OpenAI

from ..config import Config
from ..utils.logger import get_logger
from .zep_entity_reader import EntityNode, ZepEntityReader

logger = get_logger('mirofish.simulation_config')

# China daily routine configuration (Beijing time)
CHINA_TIMEZONE_CONFIG = {
    # Late night (almost no activity)
    "dead_hours": [0, 1, 2, 3, 4, 5],
    # Morning (gradually waking up)
    "morning_hours": [6, 7, 8],
    # Working hours
    "work_hours": [9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
    # Evening peak (most active)
    "peak_hours": [19, 20, 21, 22],
    # Night (activity decreases)
    "night_hours": [23],
    # Activity multipliers
    "activity_multipliers": {
        "dead": 0.05,      # Almost no one awake at late night
        "morning": 0.4,    # Gradually active in the morning
        "work": 0.7,       # Medium activity during work hours
        "peak": 1.5,       # Evening peak
        "night": 0.5       # Activity drops at night
    }
}


@dataclass
class AgentActivityConfig:
    """Agent activity configuration"""
    agent_id: int
    entity_uuid: str
    entity_name: str
    entity_type: str
    
    # Activity level (0.0-1.0)
    activity_level: float = 0.5  # Overall activity
    
    # Posting frequency (expected posts per hour)
    posts_per_hour: float = 1.0
    comments_per_hour: float = 2.0
    
    # Active hours (24-hour format, 0-23)
    active_hours: List[int] = field(default_factory=lambda: list(range(8, 23)))
    
    # Response speed (delay to hot events, in simulated minutes)
    response_delay_min: int = 5
    response_delay_max: int = 60
    
    # Sentiment bias (-1.0 to 1.0, negative to positive)
    sentiment_bias: float = 0.0
    
    # Stance (attitude towards specific topics)
    stance: str = "neutral"  # supportive, opposing, neutral, observer
    
    # Influence weight (probability that their posts are seen by other agents)
    influence_weight: float = 1.0


@dataclass  
class TimeSimulationConfig:
    """Time simulation configuration (based on Chinese daily routine)"""
    # Total simulation duration (in hours)
    total_simulation_hours: int = 72  # Default: 72 hours (3 days)
    
    # Time per round (simulated minutes) - default 60 minutes (1 hour), speeds up time flow
    minutes_per_round: int = 60
    
    # Number of agents activated per hour (range)
    agents_per_hour_min: int = 5
    agents_per_hour_max: int = 20
    
    # Peak hours (evening 19-22, most active for Chinese users)
    peak_hours: List[int] = field(default_factory=lambda: [19, 20, 21, 22])
    peak_activity_multiplier: float = 1.5
    
    # Off-peak hours (late night 0-5, almost no activity)
    off_peak_hours: List[int] = field(default_factory=lambda: [0, 1, 2, 3, 4, 5])
    off_peak_activity_multiplier: float = 0.05  # Very low activity at night
    
    # Morning hours
    morning_hours: List[int] = field(default_factory=lambda: [6, 7, 8])
    morning_activity_multiplier: float = 0.4
    
    # Working hours
    work_hours: List[int] = field(default_factory=lambda: [9, 10, 11, 12, 13, 14, 15, 16, 17, 18])
    work_activity_multiplier: float = 0.7


@dataclass
class EventConfig:
    """Event configuration"""
    # Initial events (triggered at the start of the simulation)
    initial_posts: List[Dict[str, Any]] = field(default_factory=list)
    
    # Scheduled events (triggered at specific times)
    scheduled_events: List[Dict[str, Any]] = field(default_factory=list)
    
    # Hot topic keywords
    hot_topics: List[str] = field(default_factory=list)
    
    # Narrative direction
    narrative_direction: str = ""


@dataclass
class PlatformConfig:
    """Platform-specific configuration"""
    platform: str  # twitter or reddit
    
    # Recommendation algorithm weights
    recency_weight: float = 0.4  # Recency
    popularity_weight: float = 0.3  # Popularity
    relevance_weight: float = 0.3  # Relevance
    
    # Viral threshold (number of interactions to trigger viral spread)
    viral_threshold: int = 10
    
    # Echo chamber strength (degree of similar opinions clustering)
    echo_chamber_strength: float = 0.5


@dataclass
class SimulationParameters:
    """Full simulation parameter configuration"""
    # Basic information
    simulation_id: str
    project_id: str
    graph_id: str
    simulation_requirement: str
    
    # Time configuration
    time_config: TimeSimulationConfig = field(default_factory=TimeSimulationConfig)
    
    # Agent configuration list
    agent_configs: List[AgentActivityConfig] = field(default_factory=list)
    
    # Event configuration
    event_config: EventConfig = field(default_factory=EventConfig)
    
    # Platform configuration
    twitter_config: Optional[PlatformConfig] = None
    reddit_config: Optional[PlatformConfig] = None
    
    # LLM configuration
    llm_model: str = ""
    llm_base_url: str = ""
    
    # Generation metadata
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    generation_reasoning: str = ""  # LLM reasoning explanation
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict"""
        time_dict = asdict(self.time_config)
        return {
            "simulation_id": self.simulation_id,
            "project_id": self.project_id,
            "graph_id": self.graph_id,
            "simulation_requirement": self.simulation_requirement,
            "time_config": time_dict,
            "agent_configs": [asdict(a) for a in self.agent_configs],
            "event_config": asdict(self.event_config),
            "twitter_config": asdict(self.twitter_config) if self.twitter_config else None,
            "reddit_config": asdict(self.reddit_config) if self.reddit_config else None,
            "llm_model": self.llm_model,
            "llm_base_url": self.llm_base_url,
            "generated_at": self.generated_at,
            "generation_reasoning": self.generation_reasoning,
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)


class SimulationConfigGenerator:
    """
    Trinh tao cau hinh mo phong thong minh

    Su dung LLM de phan tich yeu cau mo phong, noi dung tai lieu va thong tin thuc the trong do thi,
    tu dong tao ra cau hinh tham so mo phong toi uu.

    Ap dung chien luoc tao theo tung buoc:
    1. Tao cau hinh thoi gian va cau hinh su kien (nhe)
    2. Tao cau hinh Agent theo lo (moi lo 10-20 Agent)
    3. Tao cau hinh nen tang
    """
    
    # So ky tu toi da cua ngu canh
    MAX_CONTEXT_LENGTH = 50000
    # So Agent duoc tao trong moi lo
    AGENTS_PER_BATCH = 15
    
    # Do dai cat ngu canh cho tung buoc (so ky tu)
    TIME_CONFIG_CONTEXT_LENGTH = 10000   # Cau hinh thoi gian
    EVENT_CONFIG_CONTEXT_LENGTH = 8000   # Cau hinh su kien
    ENTITY_SUMMARY_LENGTH = 300          # Tom tat thuc the
    AGENT_SUMMARY_LENGTH = 300           # Tom tat thuc the trong cau hinh Agent
    ENTITIES_PER_TYPE_DISPLAY = 20       # So thuc the hien thi cho moi loai
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model_name: Optional[str] = None
    ):
        self.api_key = api_key or Config.LLM_API_KEY
        self.base_url = base_url or Config.LLM_BASE_URL
        self.model_name = model_name or Config.LLM_MODEL_NAME
        
        if not self.api_key:
            raise ValueError("LLM_API_KEY chua duoc cau hinh")
        
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
    
    def generate_config(
        self,
        simulation_id: str,
        project_id: str,
        graph_id: str,
        simulation_requirement: str,
        document_text: str,
        entities: List[EntityNode],
        enable_twitter: bool = True,
        enable_reddit: bool = True,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> SimulationParameters:
        """
        Tao thong minh cau hinh mo phong day du (theo tung buoc).

        Args:
            simulation_id: ID mo phong
            project_id: ID du an
            graph_id: ID do thi
            simulation_requirement: Mo ta yeu cau mo phong
            document_text: Noi dung tai lieu goc
            entities: Danh sach thuc the da duoc loc
            enable_twitter: Co bat Twitter hay khong
            enable_reddit: Co bat Reddit hay khong
            progress_callback: Ham callback tien do(current_step, total_steps, message)

        Returns:
            SimulationParameters: Tap tham so mo phong day du
        """
        logger.info(f"Bat dau tao thong minh cau hinh mo phong: simulation_id={simulation_id}, so thuc the={len(entities)}")
        
        # Tinh tong so buoc
        num_batches = math.ceil(len(entities) / self.AGENTS_PER_BATCH)
        total_steps = 3 + num_batches  # Cau hinh thoi gian + cau hinh su kien + N lo Agent + cau hinh nen tang
        current_step = 0
        
        def report_progress(step: int, message: str):
            nonlocal current_step
            current_step = step
            if progress_callback:
                progress_callback(step, total_steps, message)
            logger.info(f"[{step}/{total_steps}] {message}")
        
        # 1. Xay dung thong tin ngu canh co ban
        context = self._build_context(
            simulation_requirement=simulation_requirement,
            document_text=document_text,
            entities=entities
        )
        
        reasoning_parts = []
        
        # ========== Buoc 1: Tao cau hinh thoi gian ==========
        report_progress(1, "Dang tao cau hinh thoi gian...")
        num_entities = len(entities)
        time_config_result = self._generate_time_config(context, num_entities)
        time_config = self._parse_time_config(time_config_result, num_entities)
        reasoning_parts.append(f"Cau hinh thoi gian: {time_config_result.get('reasoning', 'Thanh cong')}")
        
        # ========== Buoc 2: Tao cau hinh su kien ==========
        report_progress(2, "Dang tao cau hinh su kien va chu de nong...")
        event_config_result = self._generate_event_config(context, simulation_requirement, entities)
        event_config = self._parse_event_config(event_config_result)
        reasoning_parts.append(f"Cau hinh su kien: {event_config_result.get('reasoning', 'Thanh cong')}")
        
        # ========== Buoc 3-N: Tao cau hinh Agent theo lo ==========
        all_agent_configs = []
        for batch_idx in range(num_batches):
            start_idx = batch_idx * self.AGENTS_PER_BATCH
            end_idx = min(start_idx + self.AGENTS_PER_BATCH, len(entities))
            batch_entities = entities[start_idx:end_idx]
            
            report_progress(
                3 + batch_idx,
                f"Dang tao cau hinh Agent ({start_idx + 1}-{end_idx}/{len(entities)})..."
            )
            
            batch_configs = self._generate_agent_configs_batch(
                context=context,
                entities=batch_entities,
                start_idx=start_idx,
                simulation_requirement=simulation_requirement
            )
            all_agent_configs.extend(batch_configs)
        
        reasoning_parts.append(f"Cau hinh Agent: Da tao thanh cong {len(all_agent_configs)} muc")
        
        # ========== Gan Agent dang bai phu hop cho cac bai viet ban dau ==========
        logger.info("Dang gan Agent dang bai phu hop cho cac bai viet ban dau...")
        event_config = self._assign_initial_post_agents(event_config, all_agent_configs)
        assigned_count = len([p for p in event_config.initial_posts if p.get("poster_agent_id") is not None])
        reasoning_parts.append(f"Gan bai viet ban dau: {assigned_count} bai viet da duoc gan nguoi dang")
        
        # ========== Buoc cuoi: Tao cau hinh nen tang ==========
        report_progress(total_steps, "Dang tao cau hinh nen tang...")
        twitter_config = None
        reddit_config = None
        
        if enable_twitter:
            twitter_config = PlatformConfig(
                platform="twitter",
                recency_weight=0.4,
                popularity_weight=0.3,
                relevance_weight=0.3,
                viral_threshold=10,
                echo_chamber_strength=0.5
            )
        
        if enable_reddit:
            reddit_config = PlatformConfig(
                platform="reddit",
                recency_weight=0.3,
                popularity_weight=0.4,
                relevance_weight=0.3,
                viral_threshold=15,
                echo_chamber_strength=0.6
            )
        
        # Xay dung tap tham so cuoi cung
        params = SimulationParameters(
            simulation_id=simulation_id,
            project_id=project_id,
            graph_id=graph_id,
            simulation_requirement=simulation_requirement,
            time_config=time_config,
            agent_configs=all_agent_configs,
            event_config=event_config,
            twitter_config=twitter_config,
            reddit_config=reddit_config,
            llm_model=self.model_name,
            llm_base_url=self.base_url,
            generation_reasoning=" | ".join(reasoning_parts)
        )
        
        logger.info(f"Da hoan tat tao cau hinh mo phong: {len(params.agent_configs)} cau hinh Agent")
        
        return params
    
    def _build_context(
        self,
        simulation_requirement: str,
        document_text: str,
        entities: List[EntityNode]
    ) -> str:
        """Xay dung ngu canh cho LLM, cat den do dai toi da."""
        
        # Tom tat thuc the
        entity_summary = self._summarize_entities(entities)
        
        # Xay dung ngu canh
        context_parts = [
            f"## Yeu cau mo phong\n{simulation_requirement}",
            f"\n## Thong tin thuc the ({len(entities)} muc)\n{entity_summary}",
        ]
        
        current_length = sum(len(p) for p in context_parts)
        remaining_length = self.MAX_CONTEXT_LENGTH - current_length - 500  # De lai 500 ky tu du phong
        
        if remaining_length > 0 and document_text:
            doc_text = document_text[:remaining_length]
            if len(document_text) > remaining_length:
                doc_text += "\n...(tai lieu da bi cat ngan)"
            context_parts.append(f"\n## Noi dung tai lieu goc\n{doc_text}")
        
        return "\n".join(context_parts)
    
    def _summarize_entities(self, entities: List[EntityNode]) -> str:
        """Tao tom tat thuc the."""
        lines = []
        
        # Nhom theo loai
        by_type: Dict[str, List[EntityNode]] = {}
        for e in entities:
            t = e.get_entity_type() or "Unknown"
            if t not in by_type:
                by_type[t] = []
            by_type[t].append(e)
        
        for entity_type, type_entities in by_type.items():
            lines.append(f"\n### {entity_type} ({len(type_entities)} muc)")
            # Su dung so luong hien thi va do dai tom tat tu cau hinh
            display_count = self.ENTITIES_PER_TYPE_DISPLAY
            summary_len = self.ENTITY_SUMMARY_LENGTH
            for e in type_entities[:display_count]:
                summary_preview = (e.summary[:summary_len] + "...") if len(e.summary) > summary_len else e.summary
                lines.append(f"- {e.name}: {summary_preview}")
            if len(type_entities) > display_count:
                lines.append(f"  ... con {len(type_entities) - display_count} muc nua")
        
        return "\n".join(lines)
    
    def _call_llm_with_retry(self, prompt: str, system_prompt: str) -> Dict[str, Any]:
        """Goi LLM co thu lai, bao gom logic sua JSON."""
        import re
        
        max_attempts = 3
        last_error = None
        
        for attempt in range(max_attempts):
            try:
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.7 - (attempt * 0.1)  # Giam temperature moi lan thu lai
                    # Khong dat max_tokens de LLM tu xuly linh hoat
                )
                
                content: str = response.choices[0].message.content or ""
                finish_reason = response.choices[0].finish_reason
                
                # Kiem tra xem co bi cat ngan khong
                if finish_reason == 'length':
                    logger.warning(f"Dau ra cua LLM bi cat ngan (lan thu {attempt+1})")
                    content = self._fix_truncated_json(content)
                
                # Thu phan tich JSON
                try:
                    return json.loads(content)
                except json.JSONDecodeError as e:
                    logger.warning(f"Phan tich JSON that bai (lan thu {attempt+1}): {str(e)[:80]}")
                    
                    # Thu sua JSON
                    fixed = self._try_fix_config_json(content)
                    if fixed:
                        return fixed
                    
                    last_error = e
                    
            except Exception as e:
                logger.warning(f"Goi LLM that bai (lan thu {attempt+1}): {str(e)[:80]}")
                last_error = e
                import time
                time.sleep(2 * (attempt + 1))
        
        raise last_error or Exception("Goi LLM that bai")
    
    def _fix_truncated_json(self, content: str) -> str:
        """Sua JSON bi cat ngan."""
        content = content.strip()
        
        # Tinh so dau ngoac chua dong
        open_braces = content.count('{') - content.count('}')
        open_brackets = content.count('[') - content.count(']')
        
        # Kiem tra xem co chuoi nao chua dong khong
        if content and content[-1] not in '",}]':
            content += '"'
        
        # Dong cac dau ngoac
        content += ']' * open_brackets
        content += '}' * open_braces
        
        return content
    
    def _try_fix_config_json(self, content: str) -> Optional[Dict[str, Any]]:
        """Thu sua JSON cau hinh."""
        import re
        
        # Sua truong hop bi cat ngan
        content = self._fix_truncated_json(content)
        
        # Trich xuat phan JSON
        json_match = re.search(r'\{[\s\S]*\}', content)
        if json_match:
            json_str = json_match.group()
            
            # Xoa ky tu xuong dong trong chuoi
            def fix_string(match):
                s = match.group(0)
                s = s.replace('\n', ' ').replace('\r', ' ')
                s = re.sub(r'\s+', ' ', s)
                return s
            
            json_str = re.sub(r'"[^"\\]*(?:\\.[^"\\]*)*"', fix_string, json_str)
            
            try:
                return json.loads(json_str)
            except:
                # Thu xoa tat ca ky tu dieu khien
                json_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', json_str)
                json_str = re.sub(r'\s+', ' ', json_str)
                try:
                    return json.loads(json_str)
                except:
                    pass
        
        return None
    
    def _generate_time_config(self, context: str, num_entities: int) -> Dict[str, Any]:
        """Tao cau hinh thoi gian."""
        # Su dung do dai cat ngu canh tu cau hinh
        context_truncated = context[:self.TIME_CONFIG_CONTEXT_LENGTH]
        
        # Tinh gia tri toi da cho phep (90% so Agent)
        max_agents_allowed = max(1, int(num_entities * 0.9))
        
        prompt = f"""Du tren yeu cau mo phong sau, hay tao cau hinh mo phong thoi gian.

{context_truncated}

## Nhiem vu
Hay tao JSON cau hinh thoi gian.

### Nguyen tac co ban (chi de tham khao, can dieu chinh linh hoat theo su kien va nhom tham gia cu the):
- Nhom nguoi dung la nguoi Trung Quoc, can phu hop voi thoi quen sinh hoat theo gio Bac Kinh
- Tu 0-5 gio sang gan nhu khong co hoat dong (he so muc do hoat dong 0.05)
- Tu 6-8 gio sang bat dau hoat dong dan (he so 0.4)
- Tu 9-18 gio la muc hoat dong trung binh trong gio lam viec (he so 0.7)
- Tu 19-22 gio toi la khung gio cao diem (he so 1.5)
- Sau 23 gio muc do hoat dong giam (he so 0.5)
- Quy luat chung: rang sang it hoat dong, buoi sang tang dan, gio lam viec trung binh, buoi toi cao diem
- **Quan trong**: Cac gia tri vi du ben duoi chi mang tinh tham khao, ban can dieu chinh khung gio cu the theo tinh chat su kien va dac diem nhom tham gia
  - Vi du: nhom sinh vien co the cao diem luc 21-23 gio; truyen thong hoat dong ca ngay; co quan chinh thuc chi hoat dong trong gio lam viec
  - Vi du: chu de nong dot xuat co the khien khuya van co thao luan, `off_peak_hours` co the rut ngan phu hop

### Dinh dang JSON tra ve (khong dung markdown)

Vi du:
{{
    "total_simulation_hours": 72,
    "minutes_per_round": 60,
    "agents_per_hour_min": 5,
    "agents_per_hour_max": 50,
    "peak_hours": [19, 20, 21, 22],
    "off_peak_hours": [0, 1, 2, 3, 4, 5],
    "morning_hours": [6, 7, 8],
    "work_hours": [9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
    "reasoning": "Giai thich cau hinh thoi gian cho su kien nay"
}}

Giai thich truong:
- total_simulation_hours (int): Tong thoi luong mo phong, 24-168 gio; su kien dot xuat thi ngan, chu de keo dai thi lau hon
- minutes_per_round (int): Thoi luong moi vong, 30-120 phut; khuyen nghi 60 phut
- agents_per_hour_min (int): So Agent toi thieu duoc kich hoat moi gio (pham vi: 1-{max_agents_allowed})
- agents_per_hour_max (int): So Agent toi da duoc kich hoat moi gio (pham vi: 1-{max_agents_allowed})
- peak_hours (mang int): Khung gio cao diem, dieu chinh theo nhom tham gia su kien
- off_peak_hours (mang int): Khung gio thap diem, thuong la nua dem ve sang
- morning_hours (mang int): Khung gio buoi sang
- work_hours (mang int): Khung gio lam viec
- reasoning (string): Giai thich ngan gon vi sao cau hinh nhu vay"""

        system_prompt = "Ban la chuyen gia mo phong mang xa hoi. Tra ve JSON thuan, cau hinh thoi gian phai phu hop voi thoi quen sinh hoat cua nguoi Trung Quoc."
        
        try:
            return self._call_llm_with_retry(prompt, system_prompt)
        except Exception as e:
            logger.warning(f"LLM tao cau hinh thoi gian that bai: {e}, se dung cau hinh mac dinh")
            return self._get_default_time_config(num_entities)
    
    def _get_default_time_config(self, num_entities: int) -> Dict[str, Any]:
        """Lay cau hinh thoi gian mac dinh (theo thoi quen sinh hoat cua nguoi Trung Quoc)."""
        return {
            "total_simulation_hours": 72,
            "minutes_per_round": 60,  # Moi vong 1 gio, tang toc dong thoi gian
            "agents_per_hour_min": max(1, num_entities // 15),
            "agents_per_hour_max": max(5, num_entities // 5),
            "peak_hours": [19, 20, 21, 22],
            "off_peak_hours": [0, 1, 2, 3, 4, 5],
            "morning_hours": [6, 7, 8],
            "work_hours": [9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
            "reasoning": "Su dung cau hinh mac dinh theo thoi quen sinh hoat cua nguoi Trung Quoc (moi vong 1 gio)"
        }
    
    def _parse_time_config(self, result: Dict[str, Any], num_entities: int) -> TimeSimulationConfig:
        """Phan tich ket qua cau hinh thoi gian va xac thuc gia tri agents_per_hour khong vuot qua tong so Agent."""
        # Lay gia tri goc
        agents_per_hour_min = result.get("agents_per_hour_min", max(1, num_entities // 15))
        agents_per_hour_max = result.get("agents_per_hour_max", max(5, num_entities // 5))
        
        # Xac thuc va dieu chinh: dam bao khong vuot qua tong so Agent
        if agents_per_hour_min > num_entities:
            logger.warning(f"agents_per_hour_min ({agents_per_hour_min}) vuot qua tong so Agent ({num_entities}), da duoc dieu chinh")
            agents_per_hour_min = max(1, num_entities // 10)
        
        if agents_per_hour_max > num_entities:
            logger.warning(f"agents_per_hour_max ({agents_per_hour_max}) vuot qua tong so Agent ({num_entities}), da duoc dieu chinh")
            agents_per_hour_max = max(agents_per_hour_min + 1, num_entities // 2)
        
        # Dam bao min < max
        if agents_per_hour_min >= agents_per_hour_max:
            agents_per_hour_min = max(1, agents_per_hour_max // 2)
            logger.warning(f"agents_per_hour_min >= max, da duoc dieu chinh thanh {agents_per_hour_min}")
        
        return TimeSimulationConfig(
            total_simulation_hours=result.get("total_simulation_hours", 72),
            minutes_per_round=result.get("minutes_per_round", 60),  # Mac dinh moi vong 1 gio
            agents_per_hour_min=agents_per_hour_min,
            agents_per_hour_max=agents_per_hour_max,
            peak_hours=result.get("peak_hours", [19, 20, 21, 22]),
            off_peak_hours=result.get("off_peak_hours", [0, 1, 2, 3, 4, 5]),
            off_peak_activity_multiplier=0.05,  # Rang sang gan nhu khong co ai
            morning_hours=result.get("morning_hours", [6, 7, 8]),
            morning_activity_multiplier=0.4,
            work_hours=result.get("work_hours", list(range(9, 19))),
            work_activity_multiplier=0.7,
            peak_activity_multiplier=1.5
        )
    
    def _generate_event_config(
        self, 
        context: str, 
        simulation_requirement: str,
        entities: List[EntityNode]
    ) -> Dict[str, Any]:
        """Tao cau hinh su kien."""
        
        # Lay danh sach loai thuc the co san de LLM tham khao
        entity_types_available = list(set(
            e.get_entity_type() or "Unknown" for e in entities
        ))
        
        # Liet ke ten thuc the tieu bieu cho tung loai
        type_examples = {}
        for e in entities:
            etype = e.get_entity_type() or "Unknown"
            if etype not in type_examples:
                type_examples[etype] = []
            if len(type_examples[etype]) < 3:
                type_examples[etype].append(e.name)
        
        type_info = "\n".join([
            f"- {t}: {', '.join(examples)}" 
            for t, examples in type_examples.items()
        ])
        
        # Su dung do dai cat ngu canh tu cau hinh
        context_truncated = context[:self.EVENT_CONFIG_CONTEXT_LENGTH]
        
        prompt = f"""Du tren yeu cau mo phong sau, hay tao cau hinh su kien.

Yeu cau mo phong: {simulation_requirement}

{context_truncated}

## Cac loai thuc the co san va vi du
{type_info}

## Nhiem vu
Hay tao JSON cau hinh su kien:
- Trich xuat cac tu khoa chu de nong
- Mo ta huong phat trien du luan
- Thiet ke noi dung bai viet ban dau, **moi bai viet bat buoc phai chi dinh `poster_type` (loai nguoi dang)**

**Quan trong**: `poster_type` bat buoc phai duoc chon tu "cac loai thuc the co san" o tren, de bai viet ban dau co the duoc gan cho Agent phu hop de dang.
Vi du: thong bao chinh thuc nen do loai Official/University dang, tin tuc do MediaOutlet dang, quan diem sinh vien do Student dang.

Dinh dang JSON tra ve (khong dung markdown):
{{
    "hot_topics": ["Tu khoa 1", "Tu khoa 2", ...],
    "narrative_direction": "<Mo ta huong phat trien du luan>",
    "initial_posts": [
        {{"content": "Noi dung bai viet", "poster_type": "Loai thuc the (bat buoc chon tu cac loai co san)"}},
        ...
    ],
    "reasoning": "<Giai thich ngan gon>"
}}"""

        system_prompt = "Ban la chuyen gia phan tich du luan. Tra ve JSON thuan. Luu y `poster_type` phai khop chinh xac voi loai thuc the co san."
        
        try:
            return self._call_llm_with_retry(prompt, system_prompt)
        except Exception as e:
            logger.warning(f"LLM tao cau hinh su kien that bai: {e}, se dung cau hinh mac dinh")
            return {
                "hot_topics": [],
                "narrative_direction": "",
                "initial_posts": [],
                "reasoning": "Su dung cau hinh mac dinh"
            }
    
    def _parse_event_config(self, result: Dict[str, Any]) -> EventConfig:
        """Phan tich ket qua cau hinh su kien."""
        return EventConfig(
            initial_posts=result.get("initial_posts", []),
            scheduled_events=[],
            hot_topics=result.get("hot_topics", []),
            narrative_direction=result.get("narrative_direction", "")
        )
    
    def _assign_initial_post_agents(
        self,
        event_config: EventConfig,
        agent_configs: List[AgentActivityConfig]
    ) -> EventConfig:
        """
        Gan Agent dang bai phu hop cho cac bai viet ban dau.

        Dua vao `poster_type` cua tung bai viet de tim `agent_id` phu hop nhat.
        """
        if not event_config.initial_posts:
            return event_config
        
        # Tao chi muc Agent theo loai thuc the
        agents_by_type: Dict[str, List[AgentActivityConfig]] = {}
        for agent in agent_configs:
            etype = agent.entity_type.lower()
            if etype not in agents_by_type:
                agents_by_type[etype] = []
            agents_by_type[etype].append(agent)
        
        # Bang anh xa loai (xu ly cac dinh dang LLM co the tra ve khac nhau)
        type_aliases = {
            "official": ["official", "university", "governmentagency", "government"],
            "university": ["university", "official"],
            "mediaoutlet": ["mediaoutlet", "media"],
            "student": ["student", "person"],
            "professor": ["professor", "expert", "teacher"],
            "alumni": ["alumni", "person"],
            "organization": ["organization", "ngo", "company", "group"],
            "person": ["person", "student", "alumni"],
        }
        
        # Ghi lai chi so Agent da dung theo tung loai de tranh lap lai cung mot Agent
        used_indices: Dict[str, int] = {}
        
        updated_posts = []
        for post in event_config.initial_posts:
            poster_type = post.get("poster_type", "").lower()
            content = post.get("content", "")
            
            # Thu tim Agent phu hop
            matched_agent_id = None
            
            # 1. Khop truc tiep
            if poster_type in agents_by_type:
                agents = agents_by_type[poster_type]
                idx = used_indices.get(poster_type, 0) % len(agents)
                matched_agent_id = agents[idx].agent_id
                used_indices[poster_type] = idx + 1
            else:
                # 2. Khop bang alias
                for alias_key, aliases in type_aliases.items():
                    if poster_type in aliases or alias_key == poster_type:
                        for alias in aliases:
                            if alias in agents_by_type:
                                agents = agents_by_type[alias]
                                idx = used_indices.get(alias, 0) % len(agents)
                                matched_agent_id = agents[idx].agent_id
                                used_indices[alias] = idx + 1
                                break
                    if matched_agent_id is not None:
                        break
            
            # 3. Neu van khong tim duoc, dung Agent co anh huong cao nhat
            if matched_agent_id is None:
                logger.warning(f"Khong tim thay Agent khop voi loai '{poster_type}', se dung Agent co anh huong cao nhat")
                if agent_configs:
                    # Sap xep theo muc do anh huong va chon Agent cao nhat
                    sorted_agents = sorted(agent_configs, key=lambda a: a.influence_weight, reverse=True)
                    matched_agent_id = sorted_agents[0].agent_id
                else:
                    matched_agent_id = 0
            
            updated_posts.append({
                "content": content,
                "poster_type": post.get("poster_type", "Unknown"),
                "poster_agent_id": matched_agent_id
            })
            
            logger.info(f"Gan bai viet ban dau: poster_type='{poster_type}' -> agent_id={matched_agent_id}")
        
        event_config.initial_posts = updated_posts
        return event_config
    
    def _generate_agent_configs_batch(
        self,
        context: str,
        entities: List[EntityNode],
        start_idx: int,
        simulation_requirement: str
    ) -> List[AgentActivityConfig]:
        """Tao cau hinh Agent theo lo."""
        
        # Xay dung thong tin thuc the (su dung do dai tom tat tu cau hinh)
        entity_list = []
        summary_len = self.AGENT_SUMMARY_LENGTH
        for i, e in enumerate(entities):
            entity_list.append({
                "agent_id": start_idx + i,
                "entity_name": e.name,
                "entity_type": e.get_entity_type() or "Unknown",
                "summary": e.summary[:summary_len] if e.summary else ""
            })
        
        prompt = f"""Du tren thong tin sau, hay tao cau hinh hoat dong mang xa hoi cho tung thuc the.

Yeu cau mo phong: {simulation_requirement}

## Danh sach thuc the
```json
{json.dumps(entity_list, ensure_ascii=False, indent=2)}
```

## Nhiem vu
Hay tao cau hinh hoat dong cho tung thuc the, luu y:
- **Thoi gian phu hop voi thoi quen sinh hoat cua nguoi Trung Quoc**: tu 0-5 gio sang gan nhu khong hoat dong, 19-22 gio toi la luc hoat dong manh nhat
- **Co quan chinh thuc** (`University`/`GovernmentAgency`): muc do hoat dong thap (0.1-0.3), hoat dong trong gio lam viec (9-17), phan ung cham (60-240 phut), anh huong cao (2.5-3.0)
- **Truyen thong** (`MediaOutlet`): muc do hoat dong trung binh (0.4-0.6), hoat dong ca ngay (8-23), phan ung nhanh (5-30 phut), anh huong cao (2.0-2.5)
- **Ca nhan** (`Student`/`Person`/`Alumni`): muc do hoat dong cao (0.6-0.9), chu yeu hoat dong buoi toi (18-23), phan ung nhanh (1-15 phut), anh huong thap (0.8-1.2)
- **Nhan vat cong chung/chuyen gia**: muc do hoat dong trung binh (0.4-0.6), anh huong tu trung binh den cao (1.5-2.0)

Dinh dang JSON tra ve (khong dung markdown):
{{
    "agent_configs": [
        {{
            "agent_id": <bat buoc giong dau vao>,
            "activity_level": <0.0-1.0>,
            "posts_per_hour": <tan suat dang bai>,
            "comments_per_hour": <tan suat binh luan>,
            "active_hours": [<danh sach gio hoat dong, co tinh den thoi quen sinh hoat cua nguoi Trung Quoc>],
            "response_delay_min": <so phut tre phan hoi toi thieu>,
            "response_delay_max": <so phut tre phan hoi toi da>,
            "sentiment_bias": <-1.0 den 1.0>,
            "stance": "<supportive/opposing/neutral/observer>",
            "influence_weight": <trong so anh huong>
        }},
        ...
    ]
}}"""

        system_prompt = "Ban la chuyen gia phan tich hanh vi mang xa hoi. Tra ve JSON thuan, cau hinh phai phu hop voi thoi quen sinh hoat cua nguoi Trung Quoc."
        
        try:
            result = self._call_llm_with_retry(prompt, system_prompt)
            llm_configs = {cfg["agent_id"]: cfg for cfg in result.get("agent_configs", [])}
        except Exception as e:
            logger.warning(f"LLM tao lo cau hinh Agent that bai: {e}, se tao theo quy tac")
            llm_configs = {}
        
        # Xay dung doi tuong AgentActivityConfig
        configs = []
        for i, entity in enumerate(entities):
            agent_id = start_idx + i
            cfg = llm_configs.get(agent_id, {})
            
            # Neu LLM khong tao ra, dung quy tac de tao
            if not cfg:
                cfg = self._generate_agent_config_by_rule(entity)
            
            config = AgentActivityConfig(
                agent_id=agent_id,
                entity_uuid=entity.uuid,
                entity_name=entity.name,
                entity_type=entity.get_entity_type() or "Unknown",
                activity_level=cfg.get("activity_level", 0.5),
                posts_per_hour=cfg.get("posts_per_hour", 0.5),
                comments_per_hour=cfg.get("comments_per_hour", 1.0),
                active_hours=cfg.get("active_hours", list(range(9, 23))),
                response_delay_min=cfg.get("response_delay_min", 5),
                response_delay_max=cfg.get("response_delay_max", 60),
                sentiment_bias=cfg.get("sentiment_bias", 0.0),
                stance=cfg.get("stance", "neutral"),
                influence_weight=cfg.get("influence_weight", 1.0)
            )
            configs.append(config)
        
        return configs
    
    def _generate_agent_config_by_rule(self, entity: EntityNode) -> Dict[str, Any]:
        """Tao cau hinh cho mot Agent theo quy tac (dua tren thoi quen sinh hoat cua nguoi Trung Quoc)."""
        entity_type = (entity.get_entity_type() or "Unknown").lower()
        
        if entity_type in ["university", "governmentagency", "ngo"]:
            # Co quan chinh thuc: hoat dong trong gio lam viec, tan suat thap, anh huong cao
            return {
                "activity_level": 0.2,
                "posts_per_hour": 0.1,
                "comments_per_hour": 0.05,
                "active_hours": list(range(9, 18)),  # 9:00-17:59
                "response_delay_min": 60,
                "response_delay_max": 240,
                "sentiment_bias": 0.0,
                "stance": "neutral",
                "influence_weight": 3.0
            }
        elif entity_type in ["mediaoutlet"]:
            # Truyen thong: hoat dong ca ngay, tan suat trung binh, anh huong cao
            return {
                "activity_level": 0.5,
                "posts_per_hour": 0.8,
                "comments_per_hour": 0.3,
                "active_hours": list(range(7, 24)),  # 7:00-23:59
                "response_delay_min": 5,
                "response_delay_max": 30,
                "sentiment_bias": 0.0,
                "stance": "observer",
                "influence_weight": 2.5
            }
        elif entity_type in ["professor", "expert", "official"]:
            # Chuyen gia/giao su: hoat dong gio lam viec + buoi toi, tan suat trung binh
            return {
                "activity_level": 0.4,
                "posts_per_hour": 0.3,
                "comments_per_hour": 0.5,
                "active_hours": list(range(8, 22)),  # 8:00-21:59
                "response_delay_min": 15,
                "response_delay_max": 90,
                "sentiment_bias": 0.0,
                "stance": "neutral",
                "influence_weight": 2.0
            }
        elif entity_type in ["student"]:
            # Sinh vien: chu yeu hoat dong buoi toi, tan suat cao
            return {
                "activity_level": 0.8,
                "posts_per_hour": 0.6,
                "comments_per_hour": 1.5,
                "active_hours": [8, 9, 10, 11, 12, 13, 18, 19, 20, 21, 22, 23],  # Buoi sang + buoi toi
                "response_delay_min": 1,
                "response_delay_max": 15,
                "sentiment_bias": 0.0,
                "stance": "neutral",
                "influence_weight": 0.8
            }
        elif entity_type in ["alumni"]:
            # Cuu sinh vien: chu yeu hoat dong buoi toi
            return {
                "activity_level": 0.6,
                "posts_per_hour": 0.4,
                "comments_per_hour": 0.8,
                "active_hours": [12, 13, 19, 20, 21, 22, 23],  # Nghi trua + buoi toi
                "response_delay_min": 5,
                "response_delay_max": 30,
                "sentiment_bias": 0.0,
                "stance": "neutral",
                "influence_weight": 1.0
            }
        else:
            # Nguoi dung thong thuong: cao diem vao buoi toi
            return {
                "activity_level": 0.7,
                "posts_per_hour": 0.5,
                "comments_per_hour": 1.2,
                "active_hours": [9, 10, 11, 12, 13, 18, 19, 20, 21, 22, 23],  # Ban ngay + buoi toi
                "response_delay_min": 2,
                "response_delay_max": 20,
                "sentiment_bias": 0.0,
                "stance": "neutral",
                "influence_weight": 1.0
            }
    

