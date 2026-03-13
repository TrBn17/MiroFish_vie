"""
Dich vu cap nhat bo nho do thi Zep.
Dong bo cac hoat dong cua Agent trong mo phong vao do thi Zep theo thoi gian thuc.
"""

import os
import time
import threading
import json
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
from queue import Queue, Empty

from zep_cloud.client import Zep

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger('mirofish.zep_graph_memory_updater')


@dataclass
class AgentActivity:
    """Ban ghi hoat dong cua Agent."""
    platform: str           # twitter / reddit
    agent_id: int
    agent_name: str
    action_type: str        # CREATE_POST, LIKE_POST, etc.
    action_args: Dict[str, Any]
    round_num: int
    timestamp: str
    
    def to_episode_text(self) -> str:
        """
        Chuyen hoat dong thanh mo ta van ban co the gui cho Zep.

        Su dung mo ta ngon ngu tu nhien de Zep co the trich xuat thuc the va quan he.
        Khong them tien to lien quan den mo phong de tranh gay nhieu cho qua trinh cap nhat do thi.
        """
        # Tao mo ta khac nhau theo tung loai hanh dong
        action_descriptions = {
            "CREATE_POST": self._describe_create_post,
            "LIKE_POST": self._describe_like_post,
            "DISLIKE_POST": self._describe_dislike_post,
            "REPOST": self._describe_repost,
            "QUOTE_POST": self._describe_quote_post,
            "FOLLOW": self._describe_follow,
            "CREATE_COMMENT": self._describe_create_comment,
            "LIKE_COMMENT": self._describe_like_comment,
            "DISLIKE_COMMENT": self._describe_dislike_comment,
            "SEARCH_POSTS": self._describe_search,
            "SEARCH_USER": self._describe_search_user,
            "MUTE": self._describe_mute,
        }
        
        describe_func = action_descriptions.get(self.action_type, self._describe_generic)
        description = describe_func()
        
        # Tra ve truc tiep theo dinh dang "ten agent: mo ta hoat dong", khong them tien to mo phong
        return f"{self.agent_name}: {description}"
    
    def _describe_create_post(self) -> str:
        content = self.action_args.get("content", "")
        if content:
            return f"Da dang mot bai viet: \"{content}\""
        return "Da dang mot bai viet"
    
    def _describe_like_post(self) -> str:
        """Thich bai viet - bao gom noi dung bai va thong tin tac gia."""
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if post_content and post_author:
            return f"Da thich bai viet cua {post_author}: \"{post_content}\""
        elif post_content:
            return f"Da thich mot bai viet: \"{post_content}\""
        elif post_author:
            return f"Da thich mot bai viet cua {post_author}"
        return "Da thich mot bai viet"
    
    def _describe_dislike_post(self) -> str:
        """Dislike bai viet - bao gom noi dung bai va thong tin tac gia."""
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if post_content and post_author:
            return f"Da dislike bai viet cua {post_author}: \"{post_content}\""
        elif post_content:
            return f"Da dislike mot bai viet: \"{post_content}\""
        elif post_author:
            return f"Da dislike mot bai viet cua {post_author}"
        return "Da dislike mot bai viet"
    
    def _describe_repost(self) -> str:
        """Chia se lai bai viet - bao gom noi dung bai goc va tac gia."""
        original_content = self.action_args.get("original_content", "")
        original_author = self.action_args.get("original_author_name", "")
        
        if original_content and original_author:
            return f"Da chia se lai bai viet cua {original_author}: \"{original_content}\""
        elif original_content:
            return f"Da chia se lai mot bai viet: \"{original_content}\""
        elif original_author:
            return f"Da chia se lai mot bai viet cua {original_author}"
        return "Da chia se lai mot bai viet"
    
    def _describe_quote_post(self) -> str:
        """Trich dan bai viet - bao gom bai goc, tac gia va binh luan kem theo."""
        original_content = self.action_args.get("original_content", "")
        original_author = self.action_args.get("original_author_name", "")
        quote_content = self.action_args.get("quote_content", "") or self.action_args.get("content", "")
        
        base = ""
        if original_content and original_author:
            base = f"Da trich dan bai viet cua {original_author} \"{original_content}\""
        elif original_content:
            base = f"Da trich dan mot bai viet \"{original_content}\""
        elif original_author:
            base = f"Da trich dan mot bai viet cua {original_author}"
        else:
            base = "Da trich dan mot bai viet"

        if quote_content:
            base += f", kem theo binh luan: \"{quote_content}\""
        return base
    
    def _describe_follow(self) -> str:
        """Theo doi nguoi dung - bao gom ten nguoi duoc theo doi."""
        target_user_name = self.action_args.get("target_user_name", "")
        
        if target_user_name:
            return f"Da theo doi nguoi dung \"{target_user_name}\""
        return "Da theo doi mot nguoi dung"
    
    def _describe_create_comment(self) -> str:
        """Dang binh luan - bao gom noi dung binh luan va thong tin bai viet lien quan."""
        content = self.action_args.get("content", "")
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if content:
            if post_content and post_author:
                return f"Da binh luan duoi bai viet cua {post_author} \"{post_content}\": \"{content}\""
            elif post_content:
                return f"Da binh luan duoi bai viet \"{post_content}\": \"{content}\""
            elif post_author:
                return f"Da binh luan duoi bai viet cua {post_author}: \"{content}\""
            return f"Da binh luan: \"{content}\""
        return "Da dang mot binh luan"
    
    def _describe_like_comment(self) -> str:
        """Thich binh luan - bao gom noi dung binh luan va tac gia."""
        comment_content = self.action_args.get("comment_content", "")
        comment_author = self.action_args.get("comment_author_name", "")
        
        if comment_content and comment_author:
            return f"Da thich binh luan cua {comment_author}: \"{comment_content}\""
        elif comment_content:
            return f"Da thich mot binh luan: \"{comment_content}\""
        elif comment_author:
            return f"Da thich mot binh luan cua {comment_author}"
        return "Da thich mot binh luan"
    
    def _describe_dislike_comment(self) -> str:
        """Dislike binh luan - bao gom noi dung binh luan va tac gia."""
        comment_content = self.action_args.get("comment_content", "")
        comment_author = self.action_args.get("comment_author_name", "")
        
        if comment_content and comment_author:
            return f"Da dislike binh luan cua {comment_author}: \"{comment_content}\""
        elif comment_content:
            return f"Da dislike mot binh luan: \"{comment_content}\""
        elif comment_author:
            return f"Da dislike mot binh luan cua {comment_author}"
        return "Da dislike mot binh luan"
    
    def _describe_search(self) -> str:
        """Tim kiem bai viet - bao gom tu khoa tim kiem."""
        query = self.action_args.get("query", "") or self.action_args.get("keyword", "")
        return f"Da tim kiem \"{query}\"" if query else "Da thuc hien tim kiem"
    
    def _describe_search_user(self) -> str:
        """Tim kiem nguoi dung - bao gom tu khoa tim kiem."""
        query = self.action_args.get("query", "") or self.action_args.get("username", "")
        return f"Da tim kiem nguoi dung \"{query}\"" if query else "Da tim kiem nguoi dung"
    
    def _describe_mute(self) -> str:
        """Chan nguoi dung - bao gom ten nguoi bi chan."""
        target_user_name = self.action_args.get("target_user_name", "")
        
        if target_user_name:
            return f"Da chan nguoi dung \"{target_user_name}\""
        return "Da chan mot nguoi dung"
    
    def _describe_generic(self) -> str:
        # Tao mo ta tong quat cho loai hanh dong khong xac dinh
        return f"Da thuc hien hanh dong {self.action_type}"


class ZepGraphMemoryUpdater:
    """
    Bo cap nhat bo nho do thi Zep.

    Theo doi file log `actions` cua mo phong va cap nhat cac hoat dong moi cua agent vao do thi Zep theo thoi gian thuc.
    Cac hoat dong duoc nhom theo nen tang, khi tich luy du `BATCH_SIZE` se gui theo lo.

    Moi hanh vi co y nghia deu duoc dong bo sang Zep. `action_args` se chua day du ngu canh nhu:
    - Noi dung bai viet duoc like/dislike
    - Noi dung bai viet duoc repost/quote
    - Ten nguoi dung duoc follow/mute
    - Noi dung binh luan duoc like/dislike
    """
    
    # Kich thuoc moi lo gui theo tung nen tang
    BATCH_SIZE = 5
    
    # Anh xa ten nen tang de hien thi tren console
    PLATFORM_DISPLAY_NAMES = {
        'twitter': 'The gioi 1',
        'reddit': 'The gioi 2',
    }
    
    # Khoang cach giua cac lan gui (giay) de tranh goi qua nhanh
    SEND_INTERVAL = 0.5
    
    # Cau hinh retry
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # giay
    
    def __init__(self, graph_id: str, api_key: Optional[str] = None):
        """
        Khoi tao updater.

        Args:
            graph_id: ID do thi Zep.
            api_key: Zep API key, tuy chon; mac dinh doc tu cau hinh.
        """
        self.graph_id = graph_id
        self.api_key = api_key or Config.ZEP_API_KEY
        
        if not self.api_key:
            raise ValueError("ZEP_API_KEY chua duoc cau hinh")
        
        self.client = Zep(api_key=self.api_key)
        
        # Hang doi hoat dong
        self._activity_queue: Queue = Queue()
        
        # Buffer hoat dong theo tung nen tang, moi nen tang tu tich luy den `BATCH_SIZE` roi gui theo lo
        self._platform_buffers: Dict[str, List[AgentActivity]] = {
            'twitter': [],
            'reddit': [],
        }
        self._buffer_lock = threading.Lock()
        
        # Co dieu khien
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        
        # Thong ke
        self._total_activities = 0  # So hoat dong da dua vao queue
        self._total_sent = 0        # So lo gui thanh cong toi Zep
        self._total_items_sent = 0  # So hoat dong gui thanh cong
        self._failed_count = 0      # So lo gui that bai
        self._skipped_count = 0     # So hoat dong bi bo qua (DO_NOTHING)
        
        logger.info(f"Da khoi tao ZepGraphMemoryUpdater: graph_id={graph_id}, batch_size={self.BATCH_SIZE}")
    
    def _get_platform_display_name(self, platform: str) -> str:
        """Lay ten hien thi cua nen tang."""
        return self.PLATFORM_DISPLAY_NAMES.get(platform.lower(), platform)
    
    def start(self):
        """Khoi dong worker thread chay nen."""
        if self._running:
            return
        
        self._running = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            daemon=True,
            name=f"ZepMemoryUpdater-{self.graph_id[:8]}"
        )
        self._worker_thread.start()
        logger.info(f"ZepGraphMemoryUpdater da khoi dong: graph_id={self.graph_id}")
    
    def stop(self):
        """Dung worker thread chay nen."""
        self._running = False
        
        # Gui cac hoat dong con lai
        self._flush_remaining()
        
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=10)
        
        logger.info(f"ZepGraphMemoryUpdater da dung: graph_id={self.graph_id}, "
                   f"total_activities={self._total_activities}, "
                   f"batches_sent={self._total_sent}, "
                   f"items_sent={self._total_items_sent}, "
                   f"failed={self._failed_count}, "
                   f"skipped={self._skipped_count}")
    
    def add_activity(self, activity: AgentActivity):
        """
        Them mot hoat dong cua agent vao queue.

        Tat ca hanh vi co y nghia deu duoc them vao queue, gom:
        - `CREATE_POST`
        - `CREATE_COMMENT`
        - `QUOTE_POST`
        - `SEARCH_POSTS`
        - `SEARCH_USER`
        - `LIKE_POST` / `DISLIKE_POST`
        - `REPOST`
        - `FOLLOW`
        - `MUTE`
        - `LIKE_COMMENT` / `DISLIKE_COMMENT`

        `action_args` chua day du ngu canh nhu noi dung bai viet, ten nguoi dung, v.v.

        Args:
            activity: Ban ghi hoat dong cua agent.
        """
        # Bo qua hanh dong `DO_NOTHING`
        if activity.action_type == "DO_NOTHING":
            self._skipped_count += 1
            return
        
        self._activity_queue.put(activity)
        self._total_activities += 1
        logger.debug(f"Da them hoat dong vao queue Zep: {activity.agent_name} - {activity.action_type}")
    
    def add_activity_from_dict(self, data: Dict[str, Any], platform: str):
        """
        Them hoat dong tu du lieu dang dict.

        Args:
            data: Du lieu dict duoc parse tu `actions.jsonl`.
            platform: Ten nen tang (`twitter`/`reddit`).
        """
        # Bo qua cac ban ghi kieu su kien
        if "event_type" in data:
            return
        
        activity = AgentActivity(
            platform=platform,
            agent_id=data.get("agent_id", 0),
            agent_name=data.get("agent_name", ""),
            action_type=data.get("action_type", ""),
            action_args=data.get("action_args", {}),
            round_num=data.get("round", 0),
            timestamp=data.get("timestamp", datetime.now().isoformat()),
        )
        
        self.add_activity(activity)
    
    def _worker_loop(self):
        """Vong lap worker chay nen - gui hoat dong theo lo cho tung nen tang."""
        while self._running or not self._activity_queue.empty():
            try:
                # Thu lay hoat dong tu queue, timeout 1 giay
                try:
                    activity = self._activity_queue.get(timeout=1)
                    
                    # Dua hoat dong vao buffer cua nen tang tuong ung
                    platform = activity.platform.lower()
                    with self._buffer_lock:
                        if platform not in self._platform_buffers:
                            self._platform_buffers[platform] = []
                        self._platform_buffers[platform].append(activity)
                        
                        # Kiem tra xem buffer cua nen tang da du kich thuoc lo chua
                        if len(self._platform_buffers[platform]) >= self.BATCH_SIZE:
                            batch = self._platform_buffers[platform][:self.BATCH_SIZE]
                            self._platform_buffers[platform] = self._platform_buffers[platform][self.BATCH_SIZE:]
                            # Gui sau khi da xu ly xong phan lock
                            self._send_batch_activities(batch, platform)
                            # Tam dung ngan de tranh gui request qua nhanh
                            time.sleep(self.SEND_INTERVAL)
                    
                except Empty:
                    pass
                    
            except Exception as e:
                logger.error(f"Loi trong worker loop: {e}")
                time.sleep(1)
    
    def _send_batch_activities(self, activities: List[AgentActivity], platform: str):
        """
        Gui hang loat hoat dong vao do thi Zep bang cach gop thanh mot doan van ban.

        Args:
            activities: Danh sach hoat dong cua agent.
            platform: Ten nen tang.
        """
        if not activities:
            return
        
        # Gop nhieu hoat dong thanh mot doan van ban, moi dong la mot hoat dong
        episode_texts = [activity.to_episode_text() for activity in activities]
        combined_text = "\n".join(episode_texts)
        
        # Gui co kem retry
        for attempt in range(self.MAX_RETRIES):
            try:
                self.client.graph.add(
                    graph_id=self.graph_id,
                    type="text",
                    data=combined_text
                )
                
                self._total_sent += 1
                self._total_items_sent += len(activities)
                display_name = self._get_platform_display_name(platform)
                logger.info(f"Da gui thanh cong {len(activities)} hoat dong cua {display_name} vao do thi {self.graph_id}")
                logger.debug(f"Xem truoc noi dung lo gui: {combined_text[:200]}...")
                return
                
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    logger.warning(f"Gui lo hoat dong toi Zep that bai (lan {attempt + 1}/{self.MAX_RETRIES}): {e}")
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                else:
                    logger.error(f"Gui lo hoat dong toi Zep that bai sau {self.MAX_RETRIES} lan thu: {e}")
                    self._failed_count += 1
    
    def _flush_remaining(self):
        """Gui cac hoat dong con lai trong queue va buffer."""
        # Truoc tien dua toan bo hoat dong con lai trong queue vao buffer
        while not self._activity_queue.empty():
            try:
                activity = self._activity_queue.get_nowait()
                platform = activity.platform.lower()
                with self._buffer_lock:
                    if platform not in self._platform_buffers:
                        self._platform_buffers[platform] = []
                    self._platform_buffers[platform].append(activity)
            except Empty:
                break
        
        # Sau do gui phan con lai cua tung buffer, ke ca khi chua du `BATCH_SIZE`
        with self._buffer_lock:
            for platform, buffer in self._platform_buffers.items():
                if buffer:
                    display_name = self._get_platform_display_name(platform)
                    logger.info(f"Dang gui {len(buffer)} hoat dong con lai cua {display_name}")
                    self._send_batch_activities(buffer, platform)
            # Xoa sach tat ca buffer
            for platform in self._platform_buffers:
                self._platform_buffers[platform] = []
    
    def get_stats(self) -> Dict[str, Any]:
        """Lay thong tin thong ke."""
        with self._buffer_lock:
            buffer_sizes = {p: len(b) for p, b in self._platform_buffers.items()}
        
        return {
            "graph_id": self.graph_id,
            "batch_size": self.BATCH_SIZE,
            "total_activities": self._total_activities,  # Tong so hoat dong da vao queue
            "batches_sent": self._total_sent,            # So lo gui thanh cong
            "items_sent": self._total_items_sent,        # Tong so hoat dong gui thanh cong
            "failed_count": self._failed_count,          # So lo gui that bai
            "skipped_count": self._skipped_count,        # So hoat dong bi bo qua (DO_NOTHING)
            "queue_size": self._activity_queue.qsize(),
            "buffer_sizes": buffer_sizes,                # Kich thuoc buffer cua tung nen tang
            "running": self._running,
        }


class ZepGraphMemoryManager:
    """
    Quan ly nhieu updater bo nho do thi Zep cho cac mo phong khac nhau.

    Moi mo phong co the so huu mot updater rieng.
    """
    
    _updaters: Dict[str, ZepGraphMemoryUpdater] = {}
    _lock = threading.Lock()
    
    @classmethod
    def create_updater(cls, simulation_id: str, graph_id: str) -> ZepGraphMemoryUpdater:
        """
        Tao updater bo nho do thi cho mot mo phong.

        Args:
            simulation_id: ID mo phong.
            graph_id: ID do thi Zep.

        Returns:
            Mot instance `ZepGraphMemoryUpdater`.
        """
        with cls._lock:
            # Neu da ton tai updater cu thi dung no truoc
            if simulation_id in cls._updaters:
                cls._updaters[simulation_id].stop()
            
            updater = ZepGraphMemoryUpdater(graph_id)
            updater.start()
            cls._updaters[simulation_id] = updater
            
            logger.info(f"Da tao updater bo nho do thi: simulation_id={simulation_id}, graph_id={graph_id}")
            return updater
    
    @classmethod
    def get_updater(cls, simulation_id: str) -> Optional[ZepGraphMemoryUpdater]:
        """Lay updater cua mo phong."""
        return cls._updaters.get(simulation_id)
    
    @classmethod
    def stop_updater(cls, simulation_id: str):
        """Dung va xoa updater cua mo phong."""
        with cls._lock:
            if simulation_id in cls._updaters:
                cls._updaters[simulation_id].stop()
                del cls._updaters[simulation_id]
                logger.info(f"Da dung updater bo nho do thi: simulation_id={simulation_id}")
    
    # Co danh dau de tranh goi `stop_all` lap lai
    _stop_all_done = False
    
    @classmethod
    def stop_all(cls):
        """Dung tat ca updater."""
        # Tranh goi lap lai
        if cls._stop_all_done:
            return
        cls._stop_all_done = True
        
        with cls._lock:
            if cls._updaters:
                for simulation_id, updater in list(cls._updaters.items()):
                    try:
                        updater.stop()
                    except Exception as e:
                        logger.error(f"Dung updater that bai: simulation_id={simulation_id}, error={e}")
                cls._updaters.clear()
            logger.info("Da dung toan bo updater bo nho do thi")
    
    @classmethod
    def get_all_stats(cls) -> Dict[str, Dict[str, Any]]:
        """Lay thong tin thong ke cua tat ca updater."""
        return {
            sim_id: updater.get_stats() 
            for sim_id, updater in cls._updaters.items()
        }
