"""
Trinh tao OASIS Agent Profile.
Chuyen cac thuc the trong do thi Zep thanh Agent Profile cho nen tang mo phong OASIS.

Cai tien chinh:
1. Goi chuc nang truy xuat Zep de bo sung thong tin cho nut.
2. Toi uu prompt de tao persona rat chi tiet.
3. Phan biet thuc the ca nhan va thuc the nhom/truu tuong.
"""

import json
import random
import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

from openai import OpenAI
from zep_cloud.client import Zep

from ..config import Config
from ..utils.logger import get_logger
from .zep_entity_reader import EntityNode, ZepEntityReader

logger = get_logger('mirofish.oasis_profile')


@dataclass
class OasisAgentProfile:
    """Cau truc du lieu OASIS Agent Profile."""
    # Truong dung chung
    user_id: int
    user_name: str
    name: str
    bio: str
    persona: str
    
    # Truong tuy chon - kieu Reddit
    karma: int = 1000
    
    # Truong tuy chon - kieu Twitter
    friend_count: int = 100
    follower_count: int = 150
    statuses_count: int = 500
    
    # Thong tin persona bo sung
    age: Optional[int] = None
    gender: Optional[str] = None
    mbti: Optional[str] = None
    country: Optional[str] = None
    profession: Optional[str] = None
    interested_topics: List[str] = field(default_factory=list)
    
    # Thong tin thuc the nguon
    source_entity_uuid: Optional[str] = None
    source_entity_type: Optional[str] = None
    
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))
    
    def to_reddit_format(self) -> Dict[str, Any]:
        """Chuyen sang dinh dang Reddit."""
        profile = {
            "user_id": self.user_id,
            "username": self.user_name,  # Thu vien OASIS yeu cau dung truong `username` khong co dau gach duoi.
            "name": self.name,
            "bio": self.bio,
            "persona": self.persona,
            "karma": self.karma,
            "created_at": self.created_at,
        }
        
        # Them thong tin persona bo sung neu co
        if self.age:
            profile["age"] = self.age
        if self.gender:
            profile["gender"] = self.gender
        if self.mbti:
            profile["mbti"] = self.mbti
        if self.country:
            profile["country"] = self.country
        if self.profession:
            profile["profession"] = self.profession
        if self.interested_topics:
            profile["interested_topics"] = self.interested_topics
        
        return profile
    
    def to_twitter_format(self) -> Dict[str, Any]:
        """Chuyen sang dinh dang Twitter."""
        profile = {
            "user_id": self.user_id,
            "username": self.user_name,  # Thu vien OASIS yeu cau dung truong `username` khong co dau gach duoi.
            "name": self.name,
            "bio": self.bio,
            "persona": self.persona,
            "friend_count": self.friend_count,
            "follower_count": self.follower_count,
            "statuses_count": self.statuses_count,
            "created_at": self.created_at,
        }
        
        # Them thong tin persona bo sung
        if self.age:
            profile["age"] = self.age
        if self.gender:
            profile["gender"] = self.gender
        if self.mbti:
            profile["mbti"] = self.mbti
        if self.country:
            profile["country"] = self.country
        if self.profession:
            profile["profession"] = self.profession
        if self.interested_topics:
            profile["interested_topics"] = self.interested_topics
        
        return profile
    
    def to_dict(self) -> Dict[str, Any]:
        """Chuyen sang dang tu dien day du."""
        return {
            "user_id": self.user_id,
            "user_name": self.user_name,
            "name": self.name,
            "bio": self.bio,
            "persona": self.persona,
            "karma": self.karma,
            "friend_count": self.friend_count,
            "follower_count": self.follower_count,
            "statuses_count": self.statuses_count,
            "age": self.age,
            "gender": self.gender,
            "mbti": self.mbti,
            "country": self.country,
            "profession": self.profession,
            "interested_topics": self.interested_topics,
            "source_entity_uuid": self.source_entity_uuid,
            "source_entity_type": self.source_entity_type,
            "created_at": self.created_at,
        }


class OasisProfileGenerator:
    """
    Trinh tao OASIS Profile.

    Chuyen cac thuc the trong do thi Zep thanh Agent Profile phuc vu mo phong OASIS.

    Tinh nang toi uu:
    1. Goi truy xuat do thi Zep de lay them ngu canh phong phu hon.
    2. Tao persona rat chi tiet, gom thong tin co ban, nghe nghiep, tinh cach, hanh vi MXH.
    3. Phan biet thuc the ca nhan va thuc the nhom/to chuc.
    """
    
    # Danh sach loai MBTI
    MBTI_TYPES = [
        "INTJ", "INTP", "ENTJ", "ENTP",
        "INFJ", "INFP", "ENFJ", "ENFP",
        "ISTJ", "ISFJ", "ESTJ", "ESFJ",
        "ISTP", "ISFP", "ESTP", "ESFP"
    ]
    
    # Danh sach quoc gia pho bien
    COUNTRIES = [
        "Trung Quốc", "Hoa Kỳ", "Vương quốc Anh", "Nhật Bản", "Đức", "Pháp",
        "Canada", "Australia", "Brazil", "Ấn Độ", "Hàn Quốc"
    ]
    
    # Nhom thuc the ca nhan can tao persona cu the
    INDIVIDUAL_ENTITY_TYPES = [
        "student", "alumni", "professor", "person", "publicfigure", 
        "expert", "faculty", "official", "journalist", "activist"
    ]
    
    # Nhom thuc the tap the/to chuc can tao persona dai dien
    GROUP_ENTITY_TYPES = [
        "university", "governmentagency", "organization", "ngo", 
        "mediaoutlet", "company", "institution", "group", "community"
    ]
    
    def __init__(
        self, 
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model_name: Optional[str] = None,
        zep_api_key: Optional[str] = None,
        graph_id: Optional[str] = None
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
        
        # Client Zep dung de truy xuat ngu canh phong phu hon
        self.zep_api_key = zep_api_key or Config.ZEP_API_KEY
        self.zep_client = None
        self.graph_id = graph_id
        
        if self.zep_api_key:
            try:
                self.zep_client = Zep(api_key=self.zep_api_key)
            except Exception as e:
                logger.warning(f"Khoi tao client Zep that bai: {e}")
    
    def generate_profile_from_entity(
        self, 
        entity: EntityNode, 
        user_id: int,
        use_llm: bool = True
    ) -> OasisAgentProfile:
        """
        Tao OASIS Agent Profile tu mot thuc the Zep.

        Args:
            entity: Nut thuc the cua Zep.
            user_id: ID nguoi dung dung trong OASIS.
            use_llm: Co dung LLM de tao persona chi tiet hay khong.

        Returns:
            OasisAgentProfile
        """
        entity_type = entity.get_entity_type() or "Thực thể"
        
        # Thong tin co ban
        name = entity.name
        user_name = self._generate_username(name)
        
        # Xay dung ngu canh
        context = self._build_entity_context(entity)
        
        if use_llm:
            # Dung LLM de tao persona chi tiet
            profile_data = self._generate_profile_with_llm(
                entity_name=name,
                entity_type=entity_type,
                entity_summary=entity.summary,
                entity_attributes=entity.attributes,
                context=context
            )
        else:
            # Dung luat de tao persona co ban
            profile_data = self._generate_profile_rule_based(
                entity_name=name,
                entity_type=entity_type,
                entity_summary=entity.summary,
                entity_attributes=entity.attributes
            )
        
        return OasisAgentProfile(
            user_id=user_id,
            user_name=user_name,
            name=name,
            bio=profile_data.get("bio", f"{entity_type}: {name}"),
            persona=profile_data.get("persona", entity.summary or f"{name} là một {entity_type}."),
            karma=profile_data.get("karma", random.randint(500, 5000)),
            friend_count=profile_data.get("friend_count", random.randint(50, 500)),
            follower_count=profile_data.get("follower_count", random.randint(100, 1000)),
            statuses_count=profile_data.get("statuses_count", random.randint(100, 2000)),
            age=profile_data.get("age"),
            gender=profile_data.get("gender"),
            mbti=profile_data.get("mbti"),
            country=profile_data.get("country"),
            profession=profile_data.get("profession"),
            interested_topics=profile_data.get("interested_topics", []),
            source_entity_uuid=entity.uuid,
            source_entity_type=entity_type,
        )
    
    def _generate_username(self, name: str) -> str:
        """Tao ten nguoi dung."""
        # Loai bo ky tu dac biet va chuyen sang chu thuong
        username = name.lower().replace(" ", "_")
        username = ''.join(c for c in username if c.isalnum() or c == '_')
        
        # Them hau to ngau nhien de tranh trung lap
        suffix = random.randint(100, 999)
        return f"{username}_{suffix}"
    
    def _search_zep_for_entity(self, entity: EntityNode) -> Dict[str, Any]:
        """
        Dung tim kiem tong hop cua Zep de lay thong tin phong phu lien quan den thuc the.

        Zep khong co san mot API tim kiem tong hop, nen can tim rieng `edges` va `nodes` roi gop ket qua.
        Hai truy van duoc chay song song de tang toc do.

        Args:
            entity: Doi tuong nut thuc the.

        Returns:
            Tu dien gom `facts`, `node_summaries`, `context`.
        """
        import concurrent.futures
        
        if not self.zep_client:
            return {"facts": [], "node_summaries": [], "context": ""}
        
        entity_name = entity.name
        
        results = {
            "facts": [],
            "node_summaries": [],
            "context": ""
        }
        
        # Can co graph_id moi co the tim kiem
        if not self.graph_id:
            logger.debug("Bo qua truy xuat Zep: chua thiet lap graph_id")
            return results
        
        comprehensive_query = f"Tat ca thong tin, hoat dong, su kien, moi quan he va boi canh lien quan den {entity_name}"
        
        def search_edges():
            """Tim edge (su kien/quan he) co kem co che retry."""
            max_retries = 3
            last_exception = None
            delay = 2.0
            
            for attempt in range(max_retries):
                try:
                    return self.zep_client.graph.search(
                        query=comprehensive_query,
                        graph_id=self.graph_id,
                        limit=30,
                        scope="edges",
                        reranker="rrf"
                    )
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.debug(f"Tim edge Zep that bai lan {attempt + 1}: {str(e)[:80]}, dang thu lai...")
                        time.sleep(delay)
                        delay *= 2
                    else:
                        logger.debug(f"Tim edge Zep van that bai sau {max_retries} lan thu: {e}")
            return None
        
        def search_nodes():
            """Tim node (tom tat thuc the) co kem co che retry."""
            max_retries = 3
            last_exception = None
            delay = 2.0
            
            for attempt in range(max_retries):
                try:
                    return self.zep_client.graph.search(
                        query=comprehensive_query,
                        graph_id=self.graph_id,
                        limit=20,
                        scope="nodes",
                        reranker="rrf"
                    )
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.debug(f"Tim node Zep that bai lan {attempt + 1}: {str(e)[:80]}, dang thu lai...")
                        time.sleep(delay)
                        delay *= 2
                    else:
                        logger.debug(f"Tim node Zep van that bai sau {max_retries} lan thu: {e}")
            return None
        
        try:
            # Chay song song hai truy van edge va node
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                edge_future = executor.submit(search_edges)
                node_future = executor.submit(search_nodes)
                
                # Lay ket qua
                edge_result = edge_future.result(timeout=30)
                node_result = node_future.result(timeout=30)
            
            # Xu ly ket qua tim edge
            all_facts = set()
            if edge_result and hasattr(edge_result, 'edges') and edge_result.edges:
                for edge in edge_result.edges:
                    if hasattr(edge, 'fact') and edge.fact:
                        all_facts.add(edge.fact)
            results["facts"] = list(all_facts)
            
            # Xu ly ket qua tim node
            all_summaries = set()
            if node_result and hasattr(node_result, 'nodes') and node_result.nodes:
                for node in node_result.nodes:
                    if hasattr(node, 'summary') and node.summary:
                        all_summaries.add(node.summary)
                    if hasattr(node, 'name') and node.name and node.name != entity_name:
                        all_summaries.add(f"Thuc the lien quan: {node.name}")
            results["node_summaries"] = list(all_summaries)
            
            # Xay dung ngu canh tong hop
            context_parts = []
            if results["facts"]:
                context_parts.append("Thong tin su kien:\n" + "\n".join(f"- {f}" for f in results["facts"][:20]))
            if results["node_summaries"]:
                context_parts.append("Thuc the lien quan:\n" + "\n".join(f"- {s}" for s in results["node_summaries"][:10]))
            results["context"] = "\n\n".join(context_parts)
            
            logger.info(f"Hoan tat truy xuat tong hop Zep cho {entity_name}: lay duoc {len(results['facts'])} su kien va {len(results['node_summaries'])} nut lien quan")
            
        except concurrent.futures.TimeoutError:
            logger.warning(f"Truy xuat Zep bi timeout ({entity_name})")
        except Exception as e:
            logger.warning(f"Truy xuat Zep that bai ({entity_name}): {e}")
        
        return results
    
    def _build_entity_context(self, entity: EntityNode) -> str:
        """
        Xay dung day du ngu canh cho thuc the.

        Bao gom:
        1. Thong tin edge cua chinh thuc the (cac su kien).
        2. Thong tin chi tiet cua cac nut lien quan.
        3. Du lieu phong phu lay them tu truy xuat tong hop cua Zep.
        """
        context_parts = []
        
        # 1. Them thong tin thuoc tinh cua thuc the
        if entity.attributes:
            attrs = []
            for key, value in entity.attributes.items():
                if value and str(value).strip():
                    attrs.append(f"- {key}: {value}")
            if attrs:
                context_parts.append("### Thuoc tinh thuc the\n" + "\n".join(attrs))
        
        # 2. Them thong tin edge lien quan (su kien/quan he)
        existing_facts = set()
        if entity.related_edges:
            relationships = []
            for edge in entity.related_edges:  # Khong gioi han so luong
                fact = edge.get("fact", "")
                edge_name = edge.get("edge_name", "")
                direction = edge.get("direction", "")
                
                if fact:
                    relationships.append(f"- {fact}")
                    existing_facts.add(fact)
                elif edge_name:
                    if direction == "outgoing":
                        relationships.append(f"- {entity.name} --[{edge_name}]--> (thuc the lien quan)")
                    else:
                        relationships.append(f"- (thuc the lien quan) --[{edge_name}]--> {entity.name}")
            
            if relationships:
                context_parts.append("### Su kien va quan he lien quan\n" + "\n".join(relationships))
        
        # 3. Them thong tin chi tiet cua cac nut lien quan
        if entity.related_nodes:
            related_info = []
            for node in entity.related_nodes:  # Khong gioi han so luong
                node_name = node.get("name", "")
                node_labels = node.get("labels", [])
                node_summary = node.get("summary", "")
                
                # Loai bo cac nhan mac dinh
                custom_labels = [l for l in node_labels if l not in ["Entity", "Node"]]
                label_str = f" ({', '.join(custom_labels)})" if custom_labels else ""
                
                if node_summary:
                    related_info.append(f"- **{node_name}**{label_str}: {node_summary}")
                else:
                    related_info.append(f"- **{node_name}**{label_str}")
            
            if related_info:
                context_parts.append("### Thong tin thuc the lien quan\n" + "\n".join(related_info))
        
        # 4. Dung truy xuat tong hop cua Zep de lay them thong tin phong phu
        zep_results = self._search_zep_for_entity(entity)
        
        if zep_results.get("facts"):
            # Loai trung: bo qua cac su kien da co san
            new_facts = [f for f in zep_results["facts"] if f not in existing_facts]
            if new_facts:
                context_parts.append("### Thong tin su kien lay tu Zep\n" + "\n".join(f"- {f}" for f in new_facts[:15]))
        
        if zep_results.get("node_summaries"):
            context_parts.append("### Cac nut lien quan lay tu Zep\n" + "\n".join(f"- {s}" for s in zep_results["node_summaries"][:10]))
        
        return "\n\n".join(context_parts)
    
    def _is_individual_entity(self, entity_type: str) -> bool:
        """Kiem tra xem day co phai thuc the ca nhan khong."""
        return entity_type.lower() in self.INDIVIDUAL_ENTITY_TYPES
    
    def _is_group_entity(self, entity_type: str) -> bool:
        """Kiem tra xem day co phai thuc the nhom/to chuc khong."""
        return entity_type.lower() in self.GROUP_ENTITY_TYPES
    
    def _generate_profile_with_llm(
        self,
        entity_name: str,
        entity_type: str,
        entity_summary: str,
        entity_attributes: Dict[str, Any],
        context: str
    ) -> Dict[str, Any]:
        """
        Dung LLM de tao persona rat chi tiet.

        Phan loai theo kieu thuc the:
        - Thuc the ca nhan: tao chan dung nhan vat cu the.
        - Thuc the nhom/to chuc: tao thiet lap tai khoan dai dien.
        """
        
        is_individual = self._is_individual_entity(entity_type)
        
        if is_individual:
            prompt = self._build_individual_persona_prompt(
                entity_name, entity_type, entity_summary, entity_attributes, context
            )
        else:
            prompt = self._build_group_persona_prompt(
                entity_name, entity_type, entity_summary, entity_attributes, context
            )

        # Thu nhieu lan cho den khi thanh cong hoac dat toi gioi han thu lai
        max_attempts = 3
        last_error = None
        
        for attempt in range(max_attempts):
            try:
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[
                        {"role": "system", "content": self._get_system_prompt(is_individual)},
                        {"role": "user", "content": prompt}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.7 - (attempt * 0.1)  # Giam nhiet do sau moi lan thu lai
                    # Khong dat max_tokens de LLM tu quyet dinh do dai phan hoi
                )
                
                content = response.choices[0].message.content
                
                # Kiem tra xem dau ra co bi cat ngan khong (finish_reason khac `stop`)
                finish_reason = response.choices[0].finish_reason
                if finish_reason == 'length':
                    logger.warning(f"Dau ra LLM bi cat ngan (lan {attempt+1}), dang thu sua...")
                    content = self._fix_truncated_json(content)
                
                # Thu parse JSON
                try:
                    result = json.loads(content)
                    
                    # Kiem tra cac truong bat buoc
                    if "bio" not in result or not result["bio"]:
                        result["bio"] = entity_summary[:200] if entity_summary else f"{entity_type}: {entity_name}"
                    if "persona" not in result or not result["persona"]:
                        result["persona"] = entity_summary or f"{entity_name} là một {entity_type}."
                    
                    return result
                    
                except json.JSONDecodeError as je:
                    logger.warning(f"Parse JSON that bai (lan {attempt+1}): {str(je)[:80]}")
                    
                    # Thu sua JSON
                    result = self._try_fix_json(content, entity_name, entity_type, entity_summary)
                    if result.get("_fixed"):
                        del result["_fixed"]
                        return result
                    
                    last_error = je
                    
            except Exception as e:
                logger.warning(f"Goi LLM that bai (lan {attempt+1}): {str(e)[:80]}")
                last_error = e
                import time
                time.sleep(1 * (attempt + 1))  # Backoff tang dan
        
        logger.warning(f"Tao persona bang LLM that bai sau {max_attempts} lan thu: {last_error}, chuyen sang tao theo luat")
        return self._generate_profile_rule_based(
            entity_name, entity_type, entity_summary, entity_attributes
        )
    
    def _fix_truncated_json(self, content: str) -> str:
        """Sua JSON bi cat ngan khi dau ra khong day du."""
        import re
        
        # Neu JSON bi cat ngang thi thu dong lai
        content = content.strip()
        
        # Dem so ngoac chua dong
        open_braces = content.count('{') - content.count('}')
        open_brackets = content.count('[') - content.count(']')
        
        # Kiem tra xem co chuoi nao chua dong khong
        # Kiem tra don gian: neu ky tu cuoi khong phai dau phay/ngoac dong thi co the chuoi da bi cat
        if content and content[-1] not in '",}]':
            # Thu dong chuoi lai
            content += '"'
        
        # Dong cac ngoac con thieu
        content += ']' * open_brackets
        content += '}' * open_braces
        
        return content
    
    def _try_fix_json(self, content: str, entity_name: str, entity_type: str, entity_summary: str = "") -> Dict[str, Any]:
        """Thu sua JSON bi hong."""
        import re
        
        # 1. Truoc tien thu sua truong hop bi cat ngang
        content = self._fix_truncated_json(content)
        
        # 2. Thu trich xuat phan JSON
        json_match = re.search(r'\{[\s\S]*\}', content)
        if json_match:
            json_str = json_match.group()
            
            # 3. Xu ly van de xuong dong ben trong chuoi
            # Tim tat ca gia tri chuoi va thay ky tu xuong dong ben trong no
            def fix_string_newlines(match):
                s = match.group(0)
                # Thay ky tu xuong dong bang khoang trang
                s = s.replace('\n', ' ').replace('\r', ' ')
                # Rut gon khoang trang thua
                s = re.sub(r'\s+', ' ', s)
                return s
            
            # Match cac gia tri chuoi trong JSON
            json_str = re.sub(r'"[^"\\]*(?:\\.[^"\\]*)*"', fix_string_newlines, json_str)
            
            # 4. Thu parse lai
            try:
                result = json.loads(json_str)
                result["_fixed"] = True
                return result
            except json.JSONDecodeError as e:
                # 5. Neu van that bai thi dung cach sua manh tay hon
                try:
                    # Xoa cac ky tu dieu khien
                    json_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', json_str)
                    # Gom tat ca khoang trang lien tiep
                    json_str = re.sub(r'\s+', ' ', json_str)
                    result = json.loads(json_str)
                    result["_fixed"] = True
                    return result
                except:
                    pass
        
        # 6. Thu trich xuat mot phan thong tin tu noi dung
        bio_match = re.search(r'"bio"\s*:\s*"([^"]*)"', content)
        persona_match = re.search(r'"persona"\s*:\s*"([^"]*)', content)  # Co the da bi cat ngang
        
        bio = bio_match.group(1) if bio_match else (entity_summary[:200] if entity_summary else f"{entity_type}: {entity_name}")
        persona = persona_match.group(1) if persona_match else (entity_summary or f"{entity_name} là một {entity_type}.")
        
        # Neu trich xuat duoc noi dung co y nghia thi danh dau la da sua
        if bio_match or persona_match:
            logger.info("Đã trích xuất được một phần thông tin từ JSON bị lỗi")
            return {
                "bio": bio,
                "persona": persona,
                "_fixed": True
            }
        
        # 7. Neu that bai hoan toan thi tra ve cau truc mac dinh
        logger.warning("Không thể sửa JSON, trả về cấu trúc mặc định")
        return {
            "bio": entity_summary[:200] if entity_summary else f"{entity_type}: {entity_name}",
            "persona": entity_summary or f"{entity_name} là một {entity_type}."
        }
    
    def _get_system_prompt(self, is_individual: bool) -> str:
        """Lay system prompt."""
        base_prompt = "Bạn là chuyên gia tạo hồ sơ người dùng mạng xã hội. Hãy tạo chân dung nhân vật chi tiết, chân thực để phục vụ mô phỏng dư luận và bám sát bối cảnh thực tế nhất có thể. Bắt buộc trả về JSON hợp lệ, mọi giá trị chuỗi không được chứa ký tự xuống dòng chưa escape. Sử dụng tiếng Việt cho toàn bộ nội dung, trừ các trường được yêu cầu dùng tiếng Anh."
        return base_prompt
    
    def _build_individual_persona_prompt(
        self,
        entity_name: str,
        entity_type: str,
        entity_summary: str,
        entity_attributes: Dict[str, Any],
        context: str
    ) -> str:
        """Tao prompt persona chi tiet cho thuc the ca nhan."""
        
        attrs_str = json.dumps(entity_attributes, ensure_ascii=False) if entity_attributes else "không có"
        context_str = context[:3000] if context else "không có ngữ cảnh bổ sung"
        
        return f"""Hãy tạo hồ sơ người dùng mạng xã hội chi tiết cho thực thể sau, bám sát dữ kiện có sẵn và tái hiện tình huống thực tế tối đa.

Tên thực thể: {entity_name}
Loại thực thể: {entity_type}
Tóm tắt thực thể: {entity_summary}
Thuộc tính thực thể: {attrs_str}

Thông tin ngữ cảnh:
{context_str}

Hãy trả về JSON gồm các trường sau:

1. bio: phần giới thiệu mạng xã hội, khoảng 200 chữ, viết bằng tiếng Việt
2. persona: mô tả chân dung nhân vật chi tiết (văn bản thuần khoảng 2000 chữ), cần bao gồm:
   - Thông tin cơ bản (tuổi, nghề nghiệp, học vấn, nơi sinh sống)
   - Bối cảnh cá nhân (trải nghiệm quan trọng, liên hệ với sự kiện, quan hệ xã hội)
   - Đặc điểm tính cách (MBTI, tính cách cốt lõi, cách biểu lộ cảm xúc)
   - Hành vi trên mạng xã hội (tần suất đăng bài, sở thích nội dung, phong cách tương tác, đặc điểm ngôn ngữ)
   - Lập trường và quan điểm (thái độ với chủ đề, điều gì dễ khiến họ tức giận hoặc xúc động)
   - Nét riêng (cửa miệng, trải nghiệm đặc biệt, sở thích cá nhân)
   - Ký ức cá nhân (phần quan trọng của persona, nêu rõ mối liên hệ với sự kiện và các hành động/phản ứng đã có)
3. age: tuổi dạng số nguyên
4. gender: giới tính, bắt buộc dùng tiếng Anh: "male" hoặc "female"
5. mbti: loại MBTI (ví dụ INTJ, ENFP)
6. country: quốc gia, viết bằng tiếng Việt (ví dụ "Việt Nam", "Trung Quốc")
7. profession: nghề nghiệp, viết bằng tiếng Việt
8. interested_topics: mảng các chủ đề quan tâm, viết bằng tiếng Việt

Lưu ý quan trọng:
- Mọi giá trị phải là chuỗi hoặc số, không dùng ký tự xuống dòng trong giá trị
- `persona` phải là một đoạn mô tả liền mạch bằng tiếng Việt
- Toàn bộ nội dung phải dùng tiếng Việt, trừ trường `gender` phải là `male` hoặc `female`
- Nội dung phải nhất quán với thông tin thực thể
- `age` phải là số nguyên hợp lệ, `gender` phải đúng một trong hai giá trị yêu cầu
"""

    def _build_group_persona_prompt(
        self,
        entity_name: str,
        entity_type: str,
        entity_summary: str,
        entity_attributes: Dict[str, Any],
        context: str
    ) -> str:
        """Tao prompt persona chi tiet cho thuc the nhom/to chuc."""
        
        attrs_str = json.dumps(entity_attributes, ensure_ascii=False) if entity_attributes else "không có"
        context_str = context[:3000] if context else "không có ngữ cảnh bổ sung"
        
        return f"""Hãy tạo thiết lập tài khoản mạng xã hội chi tiết cho tổ chức/nhóm sau, bám sát dữ kiện có sẵn và tái hiện tình huống thực tế tối đa.

Tên thực thể: {entity_name}
Loại thực thể: {entity_type}
Tóm tắt thực thể: {entity_summary}
Thuộc tính thực thể: {attrs_str}

Thông tin ngữ cảnh:
{context_str}

Hãy trả về JSON gồm các trường sau:

1. bio: phần giới thiệu tài khoản chính thức, khoảng 200 chữ, chuyên nghiệp và phù hợp ngữ cảnh
2. persona: mô tả thiết lập tài khoản chi tiết (văn bản thuần khoảng 2000 chữ), cần bao gồm:
   - Thông tin cơ bản về tổ chức (tên đầy đủ, tính chất, bối cảnh hình thành, chức năng chính)
   - Định vị tài khoản (loại tài khoản, đối tượng mục tiêu, chức năng cốt lõi)
   - Phong cách phát ngôn (đặc điểm ngôn ngữ, cách diễn đạt thường dùng, chủ đề nhạy cảm cần tránh)
   - Đặc điểm nội dung đăng tải (loại nội dung, tần suất đăng, khung giờ hoạt động)
   - Lập trường (quan điểm chính thức về chủ đề trọng tâm, cách xử lý tranh cãi)
   - Ghi chú đặc biệt (hình ảnh đại diện cho nhóm, thói quen vận hành)
   - Ký ức của tổ chức (phần quan trọng của persona, nêu rõ mối liên hệ với sự kiện và các hành động/phản ứng đã có)
3. age: cố định là 30
4. gender: cố định là "other"
5. mbti: loại MBTI dùng để mô tả phong cách tài khoản, ví dụ ISTJ cho phong cách nghiêm túc và thận trọng
6. country: quốc gia, viết bằng tiếng Việt (ví dụ "Việt Nam", "Trung Quốc")
7. profession: mô tả chức năng của tổ chức, viết bằng tiếng Việt
8. interested_topics: mảng các lĩnh vực quan tâm, viết bằng tiếng Việt

Lưu ý quan trọng:
- Mọi giá trị phải là chuỗi hoặc số, không được là `null`
- `persona` phải là một đoạn mô tả liền mạch bằng tiếng Việt, không có ký tự xuống dòng
- Toàn bộ nội dung phải dùng tiếng Việt, trừ trường `gender` phải là `other`
- `age` phải là số nguyên 30, `gender` phải đúng là chuỗi `other`
- Giọng điệu của tài khoản phải phù hợp với vai trò và định vị của tổ chức
"""
    
    def _generate_profile_rule_based(
        self,
        entity_name: str,
        entity_type: str,
        entity_summary: str,
        entity_attributes: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Tao persona co ban bang tap luat."""
        
        # Tao persona khac nhau theo loai thuc the
        entity_type_lower = entity_type.lower()
        
        if entity_type_lower in ["student", "alumni"]:
            return {
                "bio": f"{entity_type} quan tâm đến học thuật và các vấn đề xã hội.",
                "persona": f"{entity_name} là một {entity_type.lower()} thường xuyên tham gia thảo luận về học thuật và các vấn đề xã hội. Người này thích chia sẻ góc nhìn, kết nối với bạn bè và phản hồi tích cực trước các chủ đề đang được quan tâm.",
                "age": random.randint(18, 30),
                "gender": random.choice(["male", "female"]),
                "mbti": random.choice(self.MBTI_TYPES),
                "country": random.choice(self.COUNTRIES),
                "profession": "Sinh viên",
                "interested_topics": ["Giáo dục", "Vấn đề xã hội", "Công nghệ"],
            }
        
        elif entity_type_lower in ["publicfigure", "expert", "faculty"]:
            return {
                "bio": "Chuyên gia có tiếng nói và ảnh hưởng trong lĩnh vực của mình.",
                "persona": f"{entity_name} là một {entity_type.lower()} được công nhận, thường chia sẻ phân tích và quan điểm về các vấn đề quan trọng. Họ được biết đến nhờ chuyên môn vững và sức ảnh hưởng trong các cuộc thảo luận công khai.",
                "age": random.randint(35, 60),
                "gender": random.choice(["male", "female"]),
                "mbti": random.choice(["ENTJ", "INTJ", "ENTP", "INTP"]),
                "country": random.choice(self.COUNTRIES),
                "profession": entity_attributes.get("occupation", "Chuyên gia"),
                "interested_topics": ["Chính trị", "Kinh tế", "Văn hóa và xã hội"],
            }
        
        elif entity_type_lower in ["mediaoutlet", "socialmediaplatform"]:
            return {
                "bio": f"Tài khoản chính thức của {entity_name}. Cập nhật tin tức và thông báo.",
                "persona": f"{entity_name} là một thực thể truyền thông chuyên đưa tin và thúc đẩy thảo luận công khai. Tài khoản này thường xuyên cập nhật diễn biến mới, trình bày thông tin theo hướng dễ tiếp cận và tương tác với công chúng về các vấn đề thời sự.",
                "age": 30,  # Tuoi ao cua tai khoan to chuc
                "gender": "other",  # To chuc su dung `other`
                "mbti": "ISTJ",  # Phong cach nghiem tuc va than trong
                "country": "Trung Quốc",
                "profession": "Truyền thông",
                "interested_topics": ["Tin tức tổng hợp", "Thời sự", "Vấn đề công"],
            }
        
        elif entity_type_lower in ["university", "governmentagency", "ngo", "organization"]:
            return {
                "bio": f"Tài khoản chính thức của {entity_name}.",
                "persona": f"{entity_name} là một thực thể tổ chức dùng tài khoản này để truyền đạt lập trường chính thức, công bố thông tin và tương tác với các bên liên quan về những vấn đề phù hợp với chức năng của mình.",
                "age": 30,  # Tuoi ao cua tai khoan to chuc
                "gender": "other",  # To chuc su dung `other`
                "mbti": "ISTJ",  # Phong cach nghiem tuc va than trong
                "country": "Trung Quốc",
                "profession": entity_type,
                "interested_topics": ["Chính sách công", "Cộng đồng", "Thông báo chính thức"],
            }
        
        else:
            # Persona mac dinh
            return {
                "bio": entity_summary[:150] if entity_summary else f"{entity_type}: {entity_name}",
                "persona": entity_summary or f"{entity_name} là một {entity_type.lower()} đang tham gia các cuộc thảo luận xã hội.",
                "age": random.randint(25, 50),
                "gender": random.choice(["male", "female"]),
                "mbti": random.choice(self.MBTI_TYPES),
                "country": random.choice(self.COUNTRIES),
                "profession": entity_type,
                "interested_topics": ["Chủ đề chung", "Vấn đề xã hội"],
            }
    
    def set_graph_id(self, graph_id: str):
        """Dat graph_id de truy xuat Zep."""
        self.graph_id = graph_id
    
    def generate_profiles_from_entities(
        self,
        entities: List[EntityNode],
        use_llm: bool = True,
        progress_callback: Optional[callable] = None,
        graph_id: Optional[str] = None,
        parallel_count: int = 5,
        realtime_output_path: Optional[str] = None,
        output_platform: str = "reddit"
    ) -> List[OasisAgentProfile]:
        """
        Tao hang loat Agent Profile tu danh sach thuc the, co ho tro chay song song.

        Args:
            entities: Danh sach thuc the.
            use_llm: Co dung LLM de tao persona chi tiet hay khong.
            progress_callback: Ham callback tien do `(current, total, message)`.
            graph_id: ID do thi dung de truy xuat Zep va lay them ngu canh.
            parallel_count: So luong tac vu chay song song, mac dinh la 5.
            realtime_output_path: Duong dan file de ghi ket qua ngay khi tao xong tung profile.
            output_platform: Nen tang dau ra (`reddit` hoac `twitter`).

        Returns:
            Danh sach Agent Profile.
        """
        import concurrent.futures
        from threading import Lock
        
        # Dat graph_id de truy xuat Zep
        if graph_id:
            self.graph_id = graph_id
        
        total = len(entities)
        profiles = [None] * total  # Cap phat truoc de giu dung thu tu
        completed_count = [0]  # Dung list de co the cap nhat trong closure
        lock = Lock()
        
        # Ham ho tro ghi ket qua theo thoi gian thuc
        def save_profiles_realtime():
            """Luu cac profile da tao vao file theo thoi gian thuc."""
            if not realtime_output_path:
                return
            
            with lock:
                # Loc cac profile da tao xong
                existing_profiles = [p for p in profiles if p is not None]
                if not existing_profiles:
                    return
                
                try:
                    if output_platform == "reddit":
                        # Dinh dang JSON cho Reddit
                        profiles_data = [p.to_reddit_format() for p in existing_profiles]
                        with open(realtime_output_path, 'w', encoding='utf-8') as f:
                            json.dump(profiles_data, f, ensure_ascii=False, indent=2)
                    else:
                        # Dinh dang CSV cho Twitter
                        import csv
                        profiles_data = [p.to_twitter_format() for p in existing_profiles]
                        if profiles_data:
                            fieldnames = list(profiles_data[0].keys())
                            with open(realtime_output_path, 'w', encoding='utf-8', newline='') as f:
                                writer = csv.DictWriter(f, fieldnames=fieldnames)
                                writer.writeheader()
                                writer.writerows(profiles_data)
                except Exception as e:
                    logger.warning(f"Luu profile theo thoi gian thuc that bai: {e}")
        
        def generate_single_profile(idx: int, entity: EntityNode) -> tuple:
            """Ham xu ly tao mot profile don le."""
            entity_type = entity.get_entity_type() or "Thực thể"
            
            try:
                profile = self.generate_profile_from_entity(
                    entity=entity,
                    user_id=idx,
                    use_llm=use_llm
                )
                
                # In ngay persona vua tao ra console va log
                self._print_generated_profile(entity.name, entity_type, profile)
                
                return idx, profile, None
                
            except Exception as e:
                logger.error(f"Tao persona cho thuc the {entity.name} that bai: {str(e)}")
                # Tao mot profile co ban de du phong
                fallback_profile = OasisAgentProfile(
                    user_id=idx,
                    user_name=self._generate_username(entity.name),
                    name=entity.name,
                    bio=f"{entity_type}: {entity.name}",
                    persona=entity.summary or "Một người tham gia các cuộc thảo luận xã hội.",
                    source_entity_uuid=entity.uuid,
                    source_entity_type=entity_type,
                )
                return idx, fallback_profile, str(e)
        
        logger.info(f"Bắt đầu tạo song song {total} hồ sơ Agent (số luồng: {parallel_count})...")
        print(f"\n{'='*60}")
        print(f"Bắt đầu tạo hồ sơ Agent - tổng cộng {total} thực thể, số luồng: {parallel_count}")
        print(f"{'='*60}\n")
        
        # Dung thread pool de chay song song
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count) as executor:
            # Gui toan bo tac vu vao pool
            future_to_entity = {
                executor.submit(generate_single_profile, idx, entity): (idx, entity)
                for idx, entity in enumerate(entities)
            }
            
            # Thu thap ket qua
            for future in concurrent.futures.as_completed(future_to_entity):
                idx, entity = future_to_entity[future]
                entity_type = entity.get_entity_type() or "Thực thể"
                
                try:
                    result_idx, profile, error = future.result()
                    profiles[result_idx] = profile
                    
                    with lock:
                        completed_count[0] += 1
                        current = completed_count[0]
                    
                    # Ghi ket qua ra file ngay lap tuc
                    save_profiles_realtime()
                    
                    if progress_callback:
                        progress_callback(
                            current, 
                            total, 
                            f"Đã hoàn thành {current}/{total}: {entity.name} ({entity_type})"
                        )

                    if error:
                        logger.warning(f"[{current}/{total}] {entity.name} dùng hồ sơ dự phòng: {error}")
                    else:
                        logger.info(f"[{current}/{total}] Tạo hồ sơ thành công: {entity.name} ({entity_type})")
                        
                except Exception as e:
                    logger.error(f"Xay ra loi khi xu ly thuc the {entity.name}: {str(e)}")
                    with lock:
                        completed_count[0] += 1
                    profiles[idx] = OasisAgentProfile(
                        user_id=idx,
                        user_name=self._generate_username(entity.name),
                        name=entity.name,
                        bio=f"{entity_type}: {entity.name}",
                        persona=entity.summary or "Một người tham gia các cuộc thảo luận xã hội.",
                        source_entity_uuid=entity.uuid,
                        source_entity_type=entity_type,
                    )
                    # Van ghi ra file ngay ca khi dung profile du phong
                    save_profiles_realtime()
        
        print(f"\n{'='*60}")
        print(f"Tạo hồ sơ hoàn tất! Tổng số Agent đã tạo: {len([p for p in profiles if p])}")
        print(f"{'='*60}\n")
        
        return profiles
    
    def _print_generated_profile(self, entity_name: str, entity_type: str, profile: OasisAgentProfile):
        """In persona vua tao ra console voi noi dung day du, khong cat ngan."""
        separator = "-" * 70
        
        # Tao noi dung output day du, khong cat ngan
        topics_str = ', '.join(profile.interested_topics) if profile.interested_topics else 'Không có'
        
        output_lines = [
            f"\n{separator}",
            f"[Đã tạo] {entity_name} ({entity_type})",
            f"{separator}",
            f"Tên người dùng: {profile.user_name}",
            f"",
            f"【Giới thiệu】",
            f"{profile.bio}",
            f"",
            f"【Chân dung chi tiết】",
            f"{profile.persona}",
            f"",
            f"【Thuộc tính cơ bản】",
            f"Tuổi: {profile.age} | Giới tính: {profile.gender} | MBTI: {profile.mbti}",
            f"Nghề nghiệp: {profile.profession} | Quốc gia: {profile.country}",
            f"Chủ đề quan tâm: {topics_str}",
            separator
        ]
        
        output = "\n".join(output_lines)
        
        # Chi in ra console de tranh lap lai noi dung dai trong logger
        print(output)
    
    def save_profiles(
        self,
        profiles: List[OasisAgentProfile],
        file_path: str,
        platform: str = "reddit"
    ):
        """
        Luu profile vao file theo dung dinh dang cua nen tang.

        Yeu cau dinh dang cua OASIS:
        - Twitter: CSV
        - Reddit: JSON

        Args:
            profiles: Danh sach profile.
            file_path: Duong dan file.
            platform: Loai nen tang (`reddit` hoac `twitter`).
        """
        if platform == "twitter":
            self._save_twitter_csv(profiles, file_path)
        else:
            self._save_reddit_json(profiles, file_path)
    
    def _save_twitter_csv(self, profiles: List[OasisAgentProfile], file_path: str):
        """
        Luu Twitter Profile dang CSV theo dung yeu cau cua OASIS.

        Cac cot CSV OASIS Twitter yeu cau:
        - user_id: ID nguoi dung, bat dau tu 0 theo thu tu CSV.
        - name: Ten that cua nguoi dung.
        - username: Ten nguoi dung trong he thong.
        - user_char: Persona chi tiet duoc tiem vao system prompt cua agent.
        - description: Mo ta cong khai ngan gon hien thi tren ho so.

        Khac nhau giua `user_char` va `description`:
        - `user_char`: Dung noi bo cho LLM, quyet dinh cach agent suy nghi va hanh dong.
        - `description`: Hien thi ben ngoai cho nguoi dung khac.
        """
        import csv
        
        # Dam bao file co duoi `.csv`
        if not file_path.endswith('.csv'):
            file_path = file_path.replace('.json', '.csv')
        
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Ghi dong tieu de theo yeu cau OASIS
            headers = ['user_id', 'name', 'username', 'user_char', 'description']
            writer.writerow(headers)
            
            # Ghi tung dong du lieu
            for idx, profile in enumerate(profiles):
                # `user_char`: persona day du (bio + persona) de dua vao system prompt
                user_char = profile.bio
                if profile.persona and profile.persona != profile.bio:
                    user_char = f"{profile.bio} {profile.persona}"
                # Xu ly ky tu xuong dong bang cach thay bang khoang trang
                user_char = user_char.replace('\n', ' ').replace('\r', ' ')
                
                # `description`: mo ta ngan gon de hien thi ben ngoai
                description = profile.bio.replace('\n', ' ').replace('\r', ' ')
                
                row = [
                    idx,                    # user_id: ID tang dan bat dau tu 0
                    profile.name,           # name: ten that
                    profile.user_name,      # username: ten nguoi dung
                    user_char,              # user_char: persona day du dung cho LLM
                    description             # description: mo ta ngan gon hien thi cong khai
                ]
                writer.writerow(row)

        logger.info(f"Da luu {len(profiles)} Twitter Profile vao {file_path} (dinh dang OASIS CSV)")
    
    def _normalize_gender(self, gender: Optional[str]) -> str:
        """
        Chuan hoa truong `gender` ve dinh dang tieng Anh ma OASIS yeu cau.

        Gia tri hop le: `male`, `female`, `other`.
        """
        if not gender:
            return "other"
        
        gender_lower = gender.lower().strip()
        
        # Anh xa tu mot so gia tri dia phuong ve gia tri chuan
        gender_map = {
            "nam": "male",
            "nu": "female",
            "to_chuc": "other",
            "khac": "other",
            "\u7537": "male",
            "\u5973": "female",
            "\u673a\u6784": "other",
            "\u5176\u4ed6": "other",
            # Cac gia tri tieng Anh da hop le
            "male": "male",
            "female": "female",
            "other": "other",
        }
        
        return gender_map.get(gender_lower, "other")
    
    def _save_reddit_json(self, profiles: List[OasisAgentProfile], file_path: str):
        """
        Luu Reddit Profile dang JSON.

        Dinh dang can giong `to_reddit_format()` de OASIS doc dung.
        Bat buoc phai co truong `user_id`, day la khoa de `agent_graph.get_agent()` doi chieu.

        Cac truong bat buoc:
        - user_id: ID nguoi dung (so nguyen), dung de khop voi `poster_agent_id` trong `initial_posts`.
        - username: Ten nguoi dung.
        - name: Ten hien thi.
        - bio: Gioi thieu ngan.
        - persona: Persona chi tiet.
        - age: Tuoi (so nguyen).
        - gender: `male`, `female`, hoac `other`.
        - mbti: Loai MBTI.
        - country: Quoc gia.
        """
        data = []
        for idx, profile in enumerate(profiles):
            # Dung dinh dang giong `to_reddit_format()`
            item = {
                "user_id": profile.user_id if profile.user_id is not None else idx,  # Bat buoc phai co `user_id`
                "username": profile.user_name,
                "name": profile.name,
                "bio": profile.bio[:150] if profile.bio else f"{profile.name}",
                "persona": profile.persona or f"{profile.name} là người tham gia các cuộc thảo luận xã hội.",
                "karma": profile.karma if profile.karma else 1000,
                "created_at": profile.created_at,
                # Cac truong OASIS bat buoc - dam bao luon co gia tri mac dinh
                "age": profile.age if profile.age else 30,
                "gender": self._normalize_gender(profile.gender),
                "mbti": profile.mbti if profile.mbti else "ISTJ",
                "country": profile.country if profile.country else "Trung Quốc",
            }
            
            # Cac truong tuy chon
            if profile.profession:
                item["profession"] = profile.profession
            if profile.interested_topics:
                item["interested_topics"] = profile.interested_topics
            
            data.append(item)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Da luu {len(profiles)} Reddit Profile vao {file_path} (JSON, co truong user_id)")
    
    # Giu lai ten ham cu de dam bao tuong thich nguoc
    def save_profiles_to_json(
        self,
        profiles: List[OasisAgentProfile],
        file_path: str,
        platform: str = "reddit"
    ):
        """[Da deprecated] Hay dung `save_profiles()` thay the."""
        logger.warning("`save_profiles_to_json` da deprecated, vui long dung `save_profiles`")
        self.save_profiles(profiles, file_path, platform)

