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
from ..utils.llm_client import sanitize_llm_payload, is_unrecoverable_llm_request_error
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
    Trình tạo cấu hình mô phỏng thông minh

    Sử dụng LLM để phân tích yêu cầu mô phỏng, nội dung tài liệu và thông tin thực thể trong đồ thị,
    tự động tạo ra cấu hình tham số mô phỏng tối ưu.

    Áp dụng chiến lược tạo theo từng bước:
    1. Tạo cấu hình thời gian và cấu hình sự kiện (nhẹ)
    2. Tạo cấu hình Agent theo lô (mỗi lô 10-20 Agent)
    3. Tạo cấu hình nền tảng
    """
    
    # Số ký tự tối đa của ngữ cảnh
    MAX_CONTEXT_LENGTH = 50000
    # Số Agent được tạo trong mỗi lô
    AGENTS_PER_BATCH = 15
    
    # Độ dài cắt ngữ cảnh cho từng bước (số ký tự)
    TIME_CONFIG_CONTEXT_LENGTH = 10000   # Cấu hình thời gian
    EVENT_CONFIG_CONTEXT_LENGTH = 8000   # Cấu hình sự kiện
    ENTITY_SUMMARY_LENGTH = 300          # Tóm tắt thực thể
    AGENT_SUMMARY_LENGTH = 300           # Tóm tắt thực thể trong cấu hình Agent
    ENTITIES_PER_TYPE_DISPLAY = 20       # Số thực thể hiển thị cho mỗi loại
    
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
            raise ValueError("LLM_API_KEY chưa được cấu hình")
        
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=Config.LLM_TIMEOUT_SECONDS,
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
        Tạo thông minh cấu hình mô phỏng đầy đủ (theo từng bước).

        Args:
            simulation_id: ID mô phỏng
            project_id: ID dự án
            graph_id: ID đồ thị
            simulation_requirement: Mô tả yêu cầu mô phỏng
            document_text: Nội dung tài liệu gốc
            entities: Danh sách thực thể đã được lọc
            enable_twitter: Có bật Twitter hay không
            enable_reddit: Có bật Reddit hay không
            progress_callback: Hàm callback tiến độ(current_step, total_steps, message)

        Returns:
            SimulationParameters: Tập tham số mô phỏng đầy đủ
        """
        logger.info(f"Bắt đầu tạo thông minh cấu hình mô phỏng: simulation_id={simulation_id}, số thực thể={len(entities)}")
        
        # Tính tổng số bước
        num_batches = math.ceil(len(entities) / self.AGENTS_PER_BATCH)
        total_steps = 3 + num_batches  # Cấu hình thời gian + cấu hình sự kiện + N lô Agent + cấu hình nền tảng
        current_step = 0
        
        def report_progress(step: int, message: str):
            nonlocal current_step
            current_step = step
            if progress_callback:
                progress_callback(step, total_steps, message)
            logger.info(f"[{step}/{total_steps}] {message}")
        
        # 1. Xây dựng thông tin ngữ cảnh cơ bản
        context = self._build_context(
            simulation_requirement=simulation_requirement,
            document_text=document_text,
            entities=entities
        )
        
        reasoning_parts = []
        
        # ========== Bước 1: Tạo cấu hình thời gian ==========
        report_progress(1, "Đang tạo cấu hình thời gian...")
        num_entities = len(entities)
        time_config_result = self._generate_time_config(context, num_entities)
        time_config = self._parse_time_config(time_config_result, num_entities)
        reasoning_parts.append(f"Cấu hình thời gian: {time_config_result.get('reasoning', 'Thành công')}")
        
        # ========== Bước 2: Tạo cấu hình sự kiện ==========
        report_progress(2, "Đang tạo cấu hình sự kiện và chủ đề nóng...")
        event_config_result = self._generate_event_config(context, simulation_requirement, entities)
        event_config = self._parse_event_config(event_config_result)
        reasoning_parts.append(f"Cấu hình sự kiện: {event_config_result.get('reasoning', 'Thành công')}")
        
        # ========== Bước 3-N: Tạo cấu hình Agent theo lô ==========
        all_agent_configs = []
        for batch_idx in range(num_batches):
            start_idx = batch_idx * self.AGENTS_PER_BATCH
            end_idx = min(start_idx + self.AGENTS_PER_BATCH, len(entities))
            batch_entities = entities[start_idx:end_idx]
            
            report_progress(
                3 + batch_idx,
                f"Đang tạo cấu hình Agent ({start_idx + 1}-{end_idx}/{len(entities)})..."
            )
            
            batch_configs = self._generate_agent_configs_batch(
                context=context,
                entities=batch_entities,
                start_idx=start_idx,
                simulation_requirement=simulation_requirement
            )
            all_agent_configs.extend(batch_configs)
        
        reasoning_parts.append(f"Cấu hình Agent: Đã tạo thành công {len(all_agent_configs)} mục")
        
        # ========== Gán Agent đăng bài phù hợp cho các bài viết ban đầu ==========
        logger.info("Đang gán Agent đăng bài phù hợp cho các bài viết ban đầu...")
        event_config = self._assign_initial_post_agents(event_config, all_agent_configs)
        assigned_count = len([p for p in event_config.initial_posts if p.get("poster_agent_id") is not None])
        reasoning_parts.append(f"Gán bài viết ban đầu: {assigned_count} bài viết đã được gán người đăng")
        
        # ========== Bước cuối: Tạo cấu hình nền tảng ==========
        report_progress(total_steps, "Đang tạo cấu hình nền tảng...")
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
        
        # Xây dựng tập tham số cuối cùng
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
        
        logger.info(f"Đã hoàn tất tạo cấu hình mô phỏng: {len(params.agent_configs)} cấu hình Agent")
        
        return params
    
    def _build_context(
        self,
        simulation_requirement: str,
        document_text: str,
        entities: List[EntityNode]
    ) -> str:
        """Xây dựng ngữ cảnh cho LLM, cắt đến độ dài tối đa."""
        
        # Tóm tắt thực thể
        entity_summary = self._summarize_entities(entities)
        
        # Xây dựng ngữ cảnh
        context_parts = [
            f"## Yêu cầu mô phỏng\n{simulation_requirement}",
            f"\n## Thông tin thực thể ({len(entities)} mục)\n{entity_summary}",
        ]
        
        current_length = sum(len(p) for p in context_parts)
        remaining_length = self.MAX_CONTEXT_LENGTH - current_length - 500  # Để lại 500 ký tự dự phòng
        
        if remaining_length > 0 and document_text:
            doc_text = document_text[:remaining_length]
            if len(document_text) > remaining_length:
                doc_text += "\n...(tài liệu đã bị cắt ngắn)"
            context_parts.append(f"\n## Nội dung tài liệu gốc\n{doc_text}")
        
        return "\n".join(context_parts)
    
    def _summarize_entities(self, entities: List[EntityNode]) -> str:
        """Tạo tóm tắt thực thể."""
        lines = []
        
        # Nhóm theo loại
        by_type: Dict[str, List[EntityNode]] = {}
        for e in entities:
            t = e.get_entity_type() or "Unknown"
            if t not in by_type:
                by_type[t] = []
            by_type[t].append(e)
        
        for entity_type, type_entities in by_type.items():
            lines.append(f"\n### {entity_type} ({len(type_entities)} mục)")
            # Sử dụng số lượng hiển thị và độ dài tóm tắt từ cấu hình
            display_count = self.ENTITIES_PER_TYPE_DISPLAY
            summary_len = self.ENTITY_SUMMARY_LENGTH
            for e in type_entities[:display_count]:
                summary_preview = (e.summary[:summary_len] + "...") if len(e.summary) > summary_len else e.summary
                lines.append(f"- {e.name}: {summary_preview}")
            if len(type_entities) > display_count:
                lines.append(f"  ... còn {len(type_entities) - display_count} mục nữa")
        
        return "\n".join(lines)
    
    def _call_llm_with_retry(self, prompt: str, system_prompt: str) -> Dict[str, Any]:
        """Gọi LLM có thử lại, bao gồm logic sửa JSON."""
        import re
        
        max_attempts = 3
        last_error = None
        
        for attempt in range(max_attempts):
            try:
                request_kwargs = sanitize_llm_payload({
                    "model": self.model_name,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt}
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.7 - (attempt * 0.1),  # Giảm temperature mỗi lần thử lại
                    "timeout": Config.LLM_TIMEOUT_SECONDS,
                    # Không đặt max_tokens để LLM tự xử lý linh hoạt
                })
                response = self.client.chat.completions.create(**request_kwargs)
                
                content: str = response.choices[0].message.content or ""
                finish_reason = response.choices[0].finish_reason
                
                # Kiểm tra xem có bị cắt ngắn không
                if finish_reason == 'length':
                    logger.warning(f"Đầu ra của LLM bị cắt ngắn (lần thử {attempt+1})")
                    content = self._fix_truncated_json(content)
                
                # Thử phân tích JSON
                try:
                    return json.loads(content)
                except json.JSONDecodeError as e:
                    logger.warning(f"Phân tích JSON thất bại (lần thử {attempt+1}): {str(e)[:80]}")
                    
                    # Thử sửa JSON
                    fixed = self._try_fix_config_json(content)
                    if fixed:
                        return fixed
                    
                    last_error = e
                    
            except Exception as e:
                if is_unrecoverable_llm_request_error(e):
                    logger.error(f"Gọi LLM gặp lỗi request không thể phục hồi: {e}")
                    raise
                logger.warning(f"Gọi LLM thất bại (lần thử {attempt+1}): {str(e)[:80]}")
                last_error = e
                import time
                time.sleep(2 * (attempt + 1))
        
        raise last_error or Exception("Gọi LLM thất bại")
    
    def _fix_truncated_json(self, content: str) -> str:
        """Sửa JSON bị cắt ngắn."""
        content = content.strip()
        
        # Tính số dấu ngoặc chưa đóng
        open_braces = content.count('{') - content.count('}')
        open_brackets = content.count('[') - content.count(']')
        
        # Kiểm tra xem có chuỗi nào chưa đóng không
        if content and content[-1] not in '",}]':
            content += '"'
        
        # Đóng các dấu ngoặc
        content += ']' * open_brackets
        content += '}' * open_braces
        
        return content
    
    def _try_fix_config_json(self, content: str) -> Optional[Dict[str, Any]]:
        """Thử sửa JSON cấu hình."""
        import re
        
        # Sửa trường hợp bị cắt ngắn
        content = self._fix_truncated_json(content)
        
        # Trích xuất phần JSON
        json_match = re.search(r'\{[\s\S]*\}', content)
        if json_match:
            json_str = json_match.group()
            
            # Xóa ký tự xuống dòng trong chuỗi
            def fix_string(match):
                s = match.group(0)
                s = s.replace('\n', ' ').replace('\r', ' ')
                s = re.sub(r'\s+', ' ', s)
                return s
            
            json_str = re.sub(r'"[^"\\]*(?:\\.[^"\\]*)*"', fix_string, json_str)
            
            try:
                return json.loads(json_str)
            except:
                # Thử xóa tất cả ký tự điều khiển
                json_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', json_str)
                json_str = re.sub(r'\s+', ' ', json_str)
                try:
                    return json.loads(json_str)
                except:
                    pass
        
        return None
    
    def _generate_time_config(self, context: str, num_entities: int) -> Dict[str, Any]:
        """Tạo cấu hình thời gian."""
        # Sử dụng độ dài cắt ngữ cảnh từ cấu hình
        context_truncated = context[:self.TIME_CONFIG_CONTEXT_LENGTH]
        
        # Tính giá trị tối đa cho phép (90% số Agent)
        max_agents_allowed = max(1, int(num_entities * 0.9))
        
        prompt = f"""Dựa trên yêu cầu mô phỏng sau, hãy tạo cấu hình mô phỏng thời gian.

{context_truncated}

## Nhiệm vụ
Hãy tạo JSON cấu hình thời gian.

### Nguyên tắc cơ bản (chỉ để tham khảo, cần điều chỉnh linh hoạt theo sự kiện và nhóm tham gia cụ thể):
- Nhóm người dùng là người Trung Quốc, cần phù hợp với thói quen sinh hoạt theo giờ Bắc Kinh
- Từ 0-5 giờ sáng gần như không có hoạt động (hệ số mức độ hoạt động 0.05)
- Từ 6-8 giờ sáng bắt đầu hoạt động dần (hệ số 0.4)
- Từ 9-18 giờ là mức hoạt động trung bình trong giờ làm việc (hệ số 0.7)
- Từ 19-22 giờ tối là khung giờ cao điểm (hệ số 1.5)
- Sau 23 giờ mức độ hoạt động giảm (hệ số 0.5)
- Quy luật chung: rạng sáng ít hoạt động, buổi sáng tăng dần, giờ làm việc trung bình, buổi tối cao điểm
- **Quan trọng**: Các giá trị ví dụ bên dưới chỉ mang tính tham khảo, bạn cần điều chỉnh khung giờ cụ thể theo tính chất sự kiện và đặc điểm nhóm tham gia
  - Ví dụ: nhóm sinh viên có thể cao điểm lúc 21-23 giờ; truyền thông hoạt động cả ngày; cơ quan chính thức chỉ hoạt động trong giờ làm việc
  - Ví dụ: chủ đề nóng đột xuất có thể khiến khuya vẫn có thảo luận, `off_peak_hours` có thể rút ngắn phù hợp

### Định dạng JSON trả về (không dùng markdown)

Ví dụ:
{{
    "total_simulation_hours": 72,
    "minutes_per_round": 60,
    "agents_per_hour_min": 5,
    "agents_per_hour_max": 50,
    "peak_hours": [19, 20, 21, 22],
    "off_peak_hours": [0, 1, 2, 3, 4, 5],
    "morning_hours": [6, 7, 8],
    "work_hours": [9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
    "reasoning": "Giải thích cấu hình thời gian cho sự kiện này"
}}

Giải thích trường:
- total_simulation_hours (int): Tổng thời lượng mô phỏng, 24-168 giờ; sự kiện đột xuất thì ngắn, chủ đề kéo dài thì lâu hơn
- minutes_per_round (int): Thời lượng mỗi vòng, 30-120 phút; khuyến nghị 60 phút
- agents_per_hour_min (int): Số Agent tối thiểu được kích hoạt mỗi giờ (phạm vi: 1-{max_agents_allowed})
- agents_per_hour_max (int): Số Agent tối đa được kích hoạt mỗi giờ (phạm vi: 1-{max_agents_allowed})
- peak_hours (mảng int): Khung giờ cao điểm, điều chỉnh theo nhóm tham gia sự kiện
- off_peak_hours (mảng int): Khung giờ thấp điểm, thường là nửa đêm về sáng
- morning_hours (mảng int): Khung giờ buổi sáng
- work_hours (mảng int): Khung giờ làm việc
- reasoning (string): Giải thích ngắn gọn vì sao cấu hình như vậy"""

        system_prompt = "Bạn là chuyên gia mô phỏng mạng xã hội. Trả về JSON thuần, cấu hình thời gian phải phù hợp với thói quen sinh hoạt của người Trung Quốc."
        
        try:
            return self._call_llm_with_retry(prompt, system_prompt)
        except Exception as e:
            logger.warning(f"LLM tạo cấu hình thời gian thất bại: {e}, sẽ dùng cấu hình mặc định")
            return self._get_default_time_config(num_entities)
    
    def _get_default_time_config(self, num_entities: int) -> Dict[str, Any]:
        """Lấy cấu hình thời gian mặc định (theo thói quen sinh hoạt của người Trung Quốc)."""
        return {
            "total_simulation_hours": 72,
            "minutes_per_round": 60,  # Mỗi vòng 1 giờ, tăng tốc dòng thời gian
            "agents_per_hour_min": max(1, num_entities // 15),
            "agents_per_hour_max": max(5, num_entities // 5),
            "peak_hours": [19, 20, 21, 22],
            "off_peak_hours": [0, 1, 2, 3, 4, 5],
            "morning_hours": [6, 7, 8],
            "work_hours": [9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
            "reasoning": "Sử dụng cấu hình mặc định theo thói quen sinh hoạt của người Trung Quốc (mỗi vòng 1 giờ)"
        }
    
    def _parse_time_config(self, result: Dict[str, Any], num_entities: int) -> TimeSimulationConfig:
        """Phân tích kết quả cấu hình thời gian và xác thực giá trị agents_per_hour không vượt quá tổng số Agent."""
        # Lấy giá trị gốc
        agents_per_hour_min = result.get("agents_per_hour_min", max(1, num_entities // 15))
        agents_per_hour_max = result.get("agents_per_hour_max", max(5, num_entities // 5))
        
        # Xác thực và điều chỉnh: đảm bảo không vượt quá tổng số Agent
        if agents_per_hour_min > num_entities:
            logger.warning(f"agents_per_hour_min ({agents_per_hour_min}) vượt quá tổng số Agent ({num_entities}), đã được điều chỉnh")
            agents_per_hour_min = max(1, num_entities // 10)
        
        if agents_per_hour_max > num_entities:
            logger.warning(f"agents_per_hour_max ({agents_per_hour_max}) vượt quá tổng số Agent ({num_entities}), đã được điều chỉnh")
            agents_per_hour_max = max(agents_per_hour_min + 1, num_entities // 2)
        
        # Đảm bảo min < max
        if agents_per_hour_min >= agents_per_hour_max:
            agents_per_hour_min = max(1, agents_per_hour_max // 2)
            logger.warning(f"agents_per_hour_min >= max, đã được điều chỉnh thành {agents_per_hour_min}")
        
        return TimeSimulationConfig(
            total_simulation_hours=result.get("total_simulation_hours", 72),
            minutes_per_round=result.get("minutes_per_round", 60),  # Mặc định mỗi vòng 1 giờ
            agents_per_hour_min=agents_per_hour_min,
            agents_per_hour_max=agents_per_hour_max,
            peak_hours=result.get("peak_hours", [19, 20, 21, 22]),
            off_peak_hours=result.get("off_peak_hours", [0, 1, 2, 3, 4, 5]),
            off_peak_activity_multiplier=0.05,  # Rạng sáng gần như không có ai
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
        """Tạo cấu hình sự kiện."""
        
        # Lấy danh sách loại thực thể có sẵn để LLM tham khảo
        entity_types_available = list(set(
            e.get_entity_type() or "Unknown" for e in entities
        ))
        
        # Liệt kê tên thực thể tiêu biểu cho từng loại
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
        
        # Sử dụng độ dài cắt ngữ cảnh từ cấu hình
        context_truncated = context[:self.EVENT_CONFIG_CONTEXT_LENGTH]
        
        prompt = f"""Dựa trên yêu cầu mô phỏng sau, hãy tạo cấu hình sự kiện.

Yêu cầu mô phỏng: {simulation_requirement}

{context_truncated}

## Các loại thực thể có sẵn và ví dụ
{type_info}

## Nhiệm vụ
Hãy tạo JSON cấu hình sự kiện:
- Trích xuất các từ khóa chủ đề nóng
- Mô tả hướng phát triển dư luận
- Thiết kế nội dung bài viết ban đầu, **mỗi bài viết bắt buộc phải chỉ định `poster_type` (loại người đăng)**

**Quan trọng**: `poster_type` bắt buộc phải được chọn từ "các loại thực thể có sẵn" ở trên, để bài viết ban đầu có thể được gán cho Agent phù hợp để đăng.
Ví dụ: thông báo chính thức nên do loại Official/University đăng, tin tức do MediaOutlet đăng, quan điểm sinh viên do Student đăng.

Định dạng JSON trả về (không dùng markdown):
{{
    "hot_topics": ["Từ khóa 1", "Từ khóa 2", ...],
    "narrative_direction": "<Mô tả hướng phát triển dư luận>",
    "initial_posts": [
        {{"content": "Nội dung bài viết", "poster_type": "Loại thực thể (bắt buộc chọn từ các loại có sẵn)"}},
        ...
    ],
    "reasoning": "<Giải thích ngắn gọn>"
}}"""

        system_prompt = "Bạn là chuyên gia phân tích dư luận. Trả về JSON thuần. Lưu ý `poster_type` phải khớp chính xác với loại thực thể có sẵn."
        
        try:
            return self._call_llm_with_retry(prompt, system_prompt)
        except Exception as e:
            logger.warning(f"LLM tạo cấu hình sự kiện thất bại: {e}, sẽ dùng cấu hình mặc định")
            return {
                "hot_topics": [],
                "narrative_direction": "",
                "initial_posts": [],
                "reasoning": "Sử dụng cấu hình mặc định"
            }
    
    def _parse_event_config(self, result: Dict[str, Any]) -> EventConfig:
        """Phân tích kết quả cấu hình sự kiện."""
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
        Gán Agent đăng bài phù hợp cho các bài viết ban đầu.

        Dựa vào `poster_type` của từng bài viết để tìm `agent_id` phù hợp nhất.
        """
        if not event_config.initial_posts:
            return event_config
        
        # Tạo chỉ mục Agent theo loại thực thể
        agents_by_type: Dict[str, List[AgentActivityConfig]] = {}
        for agent in agent_configs:
            etype = agent.entity_type.lower()
            if etype not in agents_by_type:
                agents_by_type[etype] = []
            agents_by_type[etype].append(agent)
        
        # Bảng ánh xạ loại (xử lý các định dạng LLM có thể trả về khác nhau)
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
        
        # Ghi lại chỉ số Agent đã dùng theo từng loại để tránh lặp lại cùng một Agent
        used_indices: Dict[str, int] = {}
        
        updated_posts = []
        for post in event_config.initial_posts:
            poster_type = post.get("poster_type", "").lower()
            content = post.get("content", "")
            
            # Thử tìm Agent phù hợp
            matched_agent_id = None
            
            # 1. Khớp trực tiếp
            if poster_type in agents_by_type:
                agents = agents_by_type[poster_type]
                idx = used_indices.get(poster_type, 0) % len(agents)
                matched_agent_id = agents[idx].agent_id
                used_indices[poster_type] = idx + 1
            else:
                # 2. Khớp bằng alias
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
            
            # 3. Nếu vẫn không tìm được, dùng Agent có ảnh hưởng cao nhất
            if matched_agent_id is None:
                logger.warning(f"Không tìm thấy Agent khớp với loại '{poster_type}', sẽ dùng Agent có ảnh hưởng cao nhất")
                if agent_configs:
                    # Sắp xếp theo mức độ ảnh hưởng và chọn Agent cao nhất
                    sorted_agents = sorted(agent_configs, key=lambda a: a.influence_weight, reverse=True)
                    matched_agent_id = sorted_agents[0].agent_id
                else:
                    matched_agent_id = 0
            
            updated_posts.append({
                "content": content,
                "poster_type": post.get("poster_type", "Unknown"),
                "poster_agent_id": matched_agent_id
            })
            
            logger.info(f"Gán bài viết ban đầu: poster_type='{poster_type}' -> agent_id={matched_agent_id}")
        
        event_config.initial_posts = updated_posts
        return event_config
    
    def _generate_agent_configs_batch(
        self,
        context: str,
        entities: List[EntityNode],
        start_idx: int,
        simulation_requirement: str
    ) -> List[AgentActivityConfig]:
        """Tạo cấu hình Agent theo lô."""
        
        # Xây dựng thông tin thực thể (sử dụng độ dài tóm tắt từ cấu hình)
        entity_list = []
        summary_len = self.AGENT_SUMMARY_LENGTH
        for i, e in enumerate(entities):
            entity_list.append({
                "agent_id": start_idx + i,
                "entity_name": e.name,
                "entity_type": e.get_entity_type() or "Unknown",
                "summary": e.summary[:summary_len] if e.summary else ""
            })
        
        prompt = f"""Dựa trên thông tin sau, hãy tạo cấu hình hoạt động mạng xã hội cho từng thực thể.

Yêu cầu mô phỏng: {simulation_requirement}

## Danh sách thực thể
```json
{json.dumps(entity_list, ensure_ascii=False, indent=2)}
```

## Nhiệm vụ
Hãy tạo cấu hình hoạt động cho từng thực thể, lưu ý:
- **Thời gian phù hợp với thói quen sinh hoạt của người Trung Quốc**: từ 0-5 giờ sáng gần như không hoạt động, 19-22 giờ tối là lúc hoạt động mạnh nhất
- **Cơ quan chính thức** (`University`/`GovernmentAgency`): mức độ hoạt động thấp (0.1-0.3), hoạt động trong giờ làm việc (9-17), phản ứng chậm (60-240 phút), ảnh hưởng cao (2.5-3.0)
- **Truyền thông** (`MediaOutlet`): mức độ hoạt động trung bình (0.4-0.6), hoạt động cả ngày (8-23), phản ứng nhanh (5-30 phút), ảnh hưởng cao (2.0-2.5)
- **Cá nhân** (`Student`/`Person`/`Alumni`): mức độ hoạt động cao (0.6-0.9), chủ yếu hoạt động buổi tối (18-23), phản ứng nhanh (1-15 phút), ảnh hưởng thấp (0.8-1.2)
- **Nhân vật công chúng/chuyên gia**: mức độ hoạt động trung bình (0.4-0.6), ảnh hưởng từ trung bình đến cao (1.5-2.0)

Định dạng JSON trả về (không dùng markdown):
{{
    "agent_configs": [
        {{
            "agent_id": <bắt buộc giống đầu vào>,
            "activity_level": <0.0-1.0>,
            "posts_per_hour": <tần suất đăng bài>,
            "comments_per_hour": <tần suất bình luận>,
            "active_hours": [<danh sách giờ hoạt động, có tính đến thói quen sinh hoạt của người Trung Quốc>],
            "response_delay_min": <số phút trễ phản hồi tối thiểu>,
            "response_delay_max": <số phút trễ phản hồi tối đa>,
            "sentiment_bias": <-1.0 đến 1.0>,
            "stance": "<supportive/opposing/neutral/observer>",
            "influence_weight": <trọng số ảnh hưởng>
        }},
        ...
    ]
}}"""

        system_prompt = "Bạn là chuyên gia phân tích hành vi mạng xã hội. Trả về JSON thuần, cấu hình phải phù hợp với thói quen sinh hoạt của người Trung Quốc."
        
        try:
            result = self._call_llm_with_retry(prompt, system_prompt)
            llm_configs = {cfg["agent_id"]: cfg for cfg in result.get("agent_configs", [])}
        except Exception as e:
            logger.warning(f"LLM tạo lô cấu hình Agent thất bại: {e}, sẽ tạo theo quy tắc")
            llm_configs = {}
        
        # Xây dựng đối tượng AgentActivityConfig
        configs = []
        for i, entity in enumerate(entities):
            agent_id = start_idx + i
            cfg = llm_configs.get(agent_id, {})
            
            # Nếu LLM không tạo ra, dùng quy tắc để tạo
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
        """Tạo cấu hình cho một Agent theo quy tắc (dựa trên thói quen sinh hoạt của người Trung Quốc)."""
        entity_type = (entity.get_entity_type() or "Unknown").lower()
        
        if entity_type in ["university", "governmentagency", "ngo"]:
            # Cơ quan chính thức: hoạt động trong giờ làm việc, tần suất thấp, ảnh hưởng cao
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
            # Truyền thông: hoạt động cả ngày, tần suất trung bình, ảnh hưởng cao
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
            # Chuyên gia/giáo sư: hoạt động giờ làm việc + buổi tối, tần suất trung bình
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
            # Sinh viên: chủ yếu hoạt động buổi tối, tần suất cao
            return {
                "activity_level": 0.8,
                "posts_per_hour": 0.6,
                "comments_per_hour": 1.5,
                "active_hours": [8, 9, 10, 11, 12, 13, 18, 19, 20, 21, 22, 23],  # Buổi sáng + buổi tối
                "response_delay_min": 1,
                "response_delay_max": 15,
                "sentiment_bias": 0.0,
                "stance": "neutral",
                "influence_weight": 0.8
            }
        elif entity_type in ["alumni"]:
            # Cựu sinh viên: chủ yếu hoạt động buổi tối
            return {
                "activity_level": 0.6,
                "posts_per_hour": 0.4,
                "comments_per_hour": 0.8,
                "active_hours": [12, 13, 19, 20, 21, 22, 23],  # Nghỉ trưa + buổi tối
                "response_delay_min": 5,
                "response_delay_max": 30,
                "sentiment_bias": 0.0,
                "stance": "neutral",
                "influence_weight": 1.0
            }
        else:
            # Người dùng thông thường: cao điểm vào buổi tối
            return {
                "activity_level": 0.7,
                "posts_per_hour": 0.5,
                "comments_per_hour": 1.2,
                "active_hours": [9, 10, 11, 12, 13, 18, 19, 20, 21, 22, 23],  # Ban ngày + buổi tối
                "response_delay_min": 2,
                "response_delay_max": 20,
                "sentiment_bias": 0.0,
                "stance": "neutral",
                "influence_weight": 1.0
            }
    

