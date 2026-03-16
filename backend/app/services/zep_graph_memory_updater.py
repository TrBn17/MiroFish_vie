"""
Dịch vụ cập nhật bộ nhớ đồ thị Zep.
Đồng bộ các hoạt động của Agent trong mô phỏng vào đồ thị Zep theo thời gian thực.
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
    """Bản ghi hoạt động của Agent."""
    platform: str           # twitter / reddit
    agent_id: int
    agent_name: str
    action_type: str        # CREATE_POST, LIKE_POST, etc.
    action_args: Dict[str, Any]
    round_num: int
    timestamp: str
    
    def to_episode_text(self) -> str:
        """
        Chuyển hoạt động thành mô tả văn bản có thể gửi cho Zep.

        Sử dụng mô tả ngôn ngữ tự nhiên để Zep có thể trích xuất thực thể và quan hệ.
        Không thêm tiền tố liên quan đến mô phỏng để tránh gây nhiễu cho quá trình cập nhật đồ thị.
        """
        # Tạo mô tả khác nhau theo từng loại hành động
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
        
        # Trả về trực tiếp theo định dạng "tên agent: mô tả hoạt động", không thêm tiền tố mô phỏng
        return f"{self.agent_name}: {description}"
    
    def _describe_create_post(self) -> str:
        content = self.action_args.get("content", "")
        if content:
            return f"Đã đăng một bài viết: \"{content}\""
        return "Đã đăng một bài viết"
    
    def _describe_like_post(self) -> str:
        """Thích bài viết - bao gồm nội dung bài và thông tin tác giả."""
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if post_content and post_author:
            return f"Đã thích bài viết của {post_author}: \"{post_content}\""
        elif post_content:
            return f"Đã thích một bài viết: \"{post_content}\""
        elif post_author:
            return f"Đã thích một bài viết của {post_author}"
        return "Đã thích một bài viết"
    
    def _describe_dislike_post(self) -> str:
        """Dislike bài viết - bao gồm nội dung bài và thông tin tác giả."""
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if post_content and post_author:
            return f"Đã dislike bài viết của {post_author}: \"{post_content}\""
        elif post_content:
            return f"Đã dislike một bài viết: \"{post_content}\""
        elif post_author:
            return f"Đã dislike một bài viết của {post_author}"
        return "Đã dislike một bài viết"
    
    def _describe_repost(self) -> str:
        """Chia sẻ lại bài viết - bao gồm nội dung bài gốc và tác giả."""
        original_content = self.action_args.get("original_content", "")
        original_author = self.action_args.get("original_author_name", "")
        
        if original_content and original_author:
            return f"Đã chia sẻ lại bài viết của {original_author}: \"{original_content}\""
        elif original_content:
            return f"Đã chia sẻ lại một bài viết: \"{original_content}\""
        elif original_author:
            return f"Đã chia sẻ lại một bài viết của {original_author}"
        return "Đã chia sẻ lại một bài viết"
    
    def _describe_quote_post(self) -> str:
        """Trích dẫn bài viết - bao gồm bài gốc, tác giả và bình luận kèm theo."""
        original_content = self.action_args.get("original_content", "")
        original_author = self.action_args.get("original_author_name", "")
        quote_content = self.action_args.get("quote_content", "") or self.action_args.get("content", "")
        
        base = ""
        if original_content and original_author:
            base = f"Đã trích dẫn bài viết của {original_author} \"{original_content}\""
        elif original_content:
            base = f"Đã trích dẫn một bài viết \"{original_content}\""
        elif original_author:
            base = f"Đã trích dẫn một bài viết của {original_author}"
        else:
            base = "Đã trích dẫn một bài viết"

        if quote_content:
            base += f", kèm theo bình luận: \"{quote_content}\""
        return base
    
    def _describe_follow(self) -> str:
        """Theo dõi người dùng - bao gồm tên người được theo dõi."""
        target_user_name = self.action_args.get("target_user_name", "")
        
        if target_user_name:
            return f"Đã theo dõi người dùng \"{target_user_name}\""
        return "Đã theo dõi một người dùng"
    
    def _describe_create_comment(self) -> str:
        """Đăng bình luận - bao gồm nội dung bình luận và thông tin bài viết liên quan."""
        content = self.action_args.get("content", "")
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if content:
            if post_content and post_author:
                return f"Đã bình luận dưới bài viết của {post_author} \"{post_content}\": \"{content}\""
            elif post_content:
                return f"Đã bình luận dưới bài viết \"{post_content}\": \"{content}\""
            elif post_author:
                return f"Đã bình luận dưới bài viết của {post_author}: \"{content}\""
            return f"Đã bình luận: \"{content}\""
        return "Đã đăng một bình luận"
    
    def _describe_like_comment(self) -> str:
        """Thích bình luận - bao gồm nội dung bình luận và tác giả."""
        comment_content = self.action_args.get("comment_content", "")
        comment_author = self.action_args.get("comment_author_name", "")
        
        if comment_content and comment_author:
            return f"Đã thích bình luận của {comment_author}: \"{comment_content}\""
        elif comment_content:
            return f"Đã thích một bình luận: \"{comment_content}\""
        elif comment_author:
            return f"Đã thích một bình luận của {comment_author}"
        return "Đã thích một bình luận"
    
    def _describe_dislike_comment(self) -> str:
        """Dislike bình luận - bao gồm nội dung bình luận và tác giả."""
        comment_content = self.action_args.get("comment_content", "")
        comment_author = self.action_args.get("comment_author_name", "")
        
        if comment_content and comment_author:
            return f"Đã dislike bình luận của {comment_author}: \"{comment_content}\""
        elif comment_content:
            return f"Đã dislike một bình luận: \"{comment_content}\""
        elif comment_author:
            return f"Đã dislike một bình luận của {comment_author}"
        return "Đã dislike một bình luận"
    
    def _describe_search(self) -> str:
        """Tìm kiếm bài viết - bao gồm từ khóa tìm kiếm."""
        query = self.action_args.get("query", "") or self.action_args.get("keyword", "")
        return f"Đã tìm kiếm \"{query}\"" if query else "Đã thực hiện tìm kiếm"
    
    def _describe_search_user(self) -> str:
        """Tìm kiếm người dùng - bao gồm từ khóa tìm kiếm."""
        query = self.action_args.get("query", "") or self.action_args.get("username", "")
        return f"Đã tìm kiếm người dùng \"{query}\"" if query else "Đã tìm kiếm người dùng"
    
    def _describe_mute(self) -> str:
        """Chặn người dùng - bao gồm tên người bị chặn."""
        target_user_name = self.action_args.get("target_user_name", "")
        
        if target_user_name:
            return f"Đã chặn người dùng \"{target_user_name}\""
        return "Đã chặn một người dùng"
    
    def _describe_generic(self) -> str:
        # Tạo mô tả tổng quát cho loại hành động không xác định
        return f"Đã thực hiện hành động {self.action_type}"


class ZepGraphMemoryUpdater:
    """
    Bộ cập nhật bộ nhớ đồ thị Zep.

    Theo dõi file log `actions` của mô phỏng và cập nhật các hoạt động mới của agent vào đồ thị Zep theo thời gian thực.
    Các hoạt động được nhóm theo nền tảng, khi tích lũy đủ `BATCH_SIZE` sẽ gửi theo lô.

    Mọi hành vi có ý nghĩa đều được đồng bộ sang Zep. `action_args` sẽ chứa đầy đủ ngữ cảnh như:
    - Nội dung bài viết được like/dislike
    - Nội dung bài viết được repost/quote
    - Tên người dùng được follow/mute
    - Nội dung bình luận được like/dislike
    """
    
    # Kích thước mỗi lô gửi theo từng nền tảng
    BATCH_SIZE = 5
    
    # Ánh xạ tên nền tảng để hiển thị trên console
    PLATFORM_DISPLAY_NAMES = {
        'twitter': 'Thế giới 1',
        'reddit': 'Thế giới 2',
    }
    
    # Khoảng cách giữa các lần gửi (giây) để tránh gọi quá nhanh
    SEND_INTERVAL = 0.5
    
    # Cấu hình retry
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # giây
    
    def __init__(self, graph_id: str, api_key: Optional[str] = None):
        """
        Khởi tạo updater.

        Args:
            graph_id: ID đồ thị Zep.
            api_key: Zep API key, tùy chọn; mặc định đọc từ cấu hình.
        """
        self.graph_id = graph_id
        self.api_key = api_key or Config.ZEP_API_KEY
        
        if not self.api_key:
            raise ValueError("ZEP_API_KEY chưa được cấu hình")
        
        self.client = Zep(api_key=self.api_key)
        
        # Hàng đợi hoạt động
        self._activity_queue: Queue = Queue()
        
        # Buffer hoạt động theo từng nền tảng, mỗi nền tảng tự tích lũy đến `BATCH_SIZE` rồi gửi theo lô
        self._platform_buffers: Dict[str, List[AgentActivity]] = {
            'twitter': [],
            'reddit': [],
        }
        self._buffer_lock = threading.Lock()
        
        # Cờ điều khiển
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        
        # Thống kê
        self._total_activities = 0  # Số hoạt động đã đưa vào queue
        self._total_sent = 0        # Số lô gửi thành công tới Zep
        self._total_items_sent = 0  # Số hoạt động gửi thành công
        self._failed_count = 0      # Số lô gửi thất bại
        self._skipped_count = 0     # Số hoạt động bị bỏ qua (DO_NOTHING)
        
        logger.info(f"Đã khởi tạo ZepGraphMemoryUpdater: graph_id={graph_id}, batch_size={self.BATCH_SIZE}")
    
    def _get_platform_display_name(self, platform: str) -> str:
        """Lấy tên hiển thị của nền tảng."""
        return self.PLATFORM_DISPLAY_NAMES.get(platform.lower(), platform)
    
    def start(self):
        """Khởi động worker thread chạy nền."""
        if self._running:
            return
        
        self._running = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            daemon=True,
            name=f"ZepMemoryUpdater-{self.graph_id[:8]}"
        )
        self._worker_thread.start()
        logger.info(f"ZepGraphMemoryUpdater đã khởi động: graph_id={self.graph_id}")
    
    def stop(self):
        """Dừng worker thread chạy nền."""
        self._running = False
        
        # Gửi các hoạt động còn lại
        self._flush_remaining()
        
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=10)
        
        logger.info(f"ZepGraphMemoryUpdater đã dừng: graph_id={self.graph_id}, "
                   f"total_activities={self._total_activities}, "
                   f"batches_sent={self._total_sent}, "
                   f"items_sent={self._total_items_sent}, "
                   f"failed={self._failed_count}, "
                   f"skipped={self._skipped_count}")
    
    def add_activity(self, activity: AgentActivity):
        """
        Thêm một hoạt động của agent vào queue.

        Tất cả hành vi có ý nghĩa đều được thêm vào queue, gồm:
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

        `action_args` chứa đầy đủ ngữ cảnh như nội dung bài viết, tên người dùng, v.v.

        Args:
            activity: Bản ghi hoạt động của agent.
        """
        # Bỏ qua hành động `DO_NOTHING`
        if activity.action_type == "DO_NOTHING":
            self._skipped_count += 1
            return
        
        self._activity_queue.put(activity)
        self._total_activities += 1
        logger.debug(f"Đã thêm hoạt động vào queue Zep: {activity.agent_name} - {activity.action_type}")
    
    def add_activity_from_dict(self, data: Dict[str, Any], platform: str):
        """
        Thêm hoạt động từ dữ liệu dạng dict.

        Args:
            data: Dữ liệu dict được parse từ `actions.jsonl`.
            platform: Tên nền tảng (`twitter`/`reddit`).
        """
        # Bỏ qua các bản ghi kiểu sự kiện
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
        """Vòng lặp worker chạy nền - gửi hoạt động theo lô cho từng nền tảng."""
        while self._running or not self._activity_queue.empty():
            try:
                # Thử lấy hoạt động từ queue, timeout 1 giây
                try:
                    activity = self._activity_queue.get(timeout=1)
                    
                    # Đưa hoạt động vào buffer của nền tảng tương ứng
                    platform = activity.platform.lower()
                    with self._buffer_lock:
                        if platform not in self._platform_buffers:
                            self._platform_buffers[platform] = []
                        self._platform_buffers[platform].append(activity)
                        
                        # Kiểm tra xem buffer của nền tảng đã đủ kích thước lô chưa
                        if len(self._platform_buffers[platform]) >= self.BATCH_SIZE:
                            batch = self._platform_buffers[platform][:self.BATCH_SIZE]
                            self._platform_buffers[platform] = self._platform_buffers[platform][self.BATCH_SIZE:]
                            # Gửi sau khi đã xử lý xong phần lock
                            self._send_batch_activities(batch, platform)
                            # Tạm dừng ngắn để tránh gửi request quá nhanh
                            time.sleep(self.SEND_INTERVAL)
                    
                except Empty:
                    pass
                    
            except Exception as e:
                logger.error(f"Lỗi trong worker loop: {e}")
                time.sleep(1)
    
    def _send_batch_activities(self, activities: List[AgentActivity], platform: str):
        """
        Gửi hàng loạt hoạt động vào đồ thị Zep bằng cách gộp thành một đoạn văn bản.

        Args:
            activities: Danh sách hoạt động của agent.
            platform: Tên nền tảng.
        """
        if not activities:
            return
        
        # Gộp nhiều hoạt động thành một đoạn văn bản, mỗi dòng là một hoạt động
        episode_texts = [activity.to_episode_text() for activity in activities]
        combined_text = "\n".join(episode_texts)
        
        # Gửi có kèm retry
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
                logger.info(f"Đã gửi thành công {len(activities)} hoạt động của {display_name} vào đồ thị {self.graph_id}")
                logger.debug(f"Xem trước nội dung lô gửi: {combined_text[:200]}...")
                return
                
            except Exception as e:
                err_msg = str(e).lower()
                body = getattr(e, "body", None)
                body_str = str(body).lower() if body else ""
                is_usage_limit = (
                    getattr(e, "status_code", None) == 403
                    or "episode usage limit" in err_msg
                    or "episode usage limit" in body_str
                )
                if is_usage_limit:
                    logger.warning(
                        "Zep: Tài khoản vượt giới hạn episode (usage limit). "
                        "Không retry. Nâng cấp plan hoặc đợi reset hạn mức."
                    )
                    self._failed_count += 1
                    return
                if attempt < self.MAX_RETRIES - 1:
                    logger.warning(f"Gửi lô hoạt động tới Zep thất bại (lần {attempt + 1}/{self.MAX_RETRIES}): {e}")
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                else:
                    logger.error(f"Gửi lô hoạt động tới Zep thất bại sau {self.MAX_RETRIES} lần thử: {e}")
                    self._failed_count += 1
    
    def _flush_remaining(self):
        """Gửi các hoạt động còn lại trong queue và buffer."""
        # Trước tiên đưa toàn bộ hoạt động còn lại trong queue vào buffer
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
        
        # Sau đó gửi phần còn lại của từng buffer, kể cả khi chưa đủ `BATCH_SIZE`
        with self._buffer_lock:
            for platform, buffer in self._platform_buffers.items():
                if buffer:
                    display_name = self._get_platform_display_name(platform)
                    logger.info(f"Đang gửi {len(buffer)} hoạt động còn lại của {display_name}")
                    self._send_batch_activities(buffer, platform)
            # Xóa sạch tất cả buffer
            for platform in self._platform_buffers:
                self._platform_buffers[platform] = []
    
    def get_stats(self) -> Dict[str, Any]:
        """Lấy thông tin thống kê."""
        with self._buffer_lock:
            buffer_sizes = {p: len(b) for p, b in self._platform_buffers.items()}
        
        return {
            "graph_id": self.graph_id,
            "batch_size": self.BATCH_SIZE,
            "total_activities": self._total_activities,  # Tổng số hoạt động đã vào queue
            "batches_sent": self._total_sent,            # Số lô gửi thành công
            "items_sent": self._total_items_sent,        # Tổng số hoạt động gửi thành công
            "failed_count": self._failed_count,          # Số lô gửi thất bại
            "skipped_count": self._skipped_count,        # Số hoạt động bị bỏ qua (DO_NOTHING)
            "queue_size": self._activity_queue.qsize(),
            "buffer_sizes": buffer_sizes,                # Kích thước buffer của từng nền tảng
            "running": self._running,
        }


class ZepGraphMemoryManager:
    """
    Quản lý nhiều updater bộ nhớ đồ thị Zep cho các mô phỏng khác nhau.

    Mỗi mô phỏng có thể sở hữu một updater riêng.
    """
    
    _updaters: Dict[str, ZepGraphMemoryUpdater] = {}
    _lock = threading.Lock()
    
    @classmethod
    def create_updater(cls, simulation_id: str, graph_id: str) -> ZepGraphMemoryUpdater:
        """
        Tạo updater bộ nhớ đồ thị cho một mô phỏng.

        Args:
            simulation_id: ID mô phỏng.
            graph_id: ID đồ thị Zep.

        Returns:
            Một instance `ZepGraphMemoryUpdater`.
        """
        with cls._lock:
            # Nếu đã tồn tại updater cũ thì dừng nó trước
            if simulation_id in cls._updaters:
                cls._updaters[simulation_id].stop()
            
            updater = ZepGraphMemoryUpdater(graph_id)
            updater.start()
            cls._updaters[simulation_id] = updater
            
            logger.info(f"Đã tạo updater bộ nhớ đồ thị: simulation_id={simulation_id}, graph_id={graph_id}")
            return updater
    
    @classmethod
    def get_updater(cls, simulation_id: str) -> Optional[ZepGraphMemoryUpdater]:
        """Lấy updater của mô phỏng."""
        return cls._updaters.get(simulation_id)
    
    @classmethod
    def stop_updater(cls, simulation_id: str):
        """Dừng và xóa updater của mô phỏng."""
        with cls._lock:
            if simulation_id in cls._updaters:
                cls._updaters[simulation_id].stop()
                del cls._updaters[simulation_id]
                logger.info(f"Đã dừng updater bộ nhớ đồ thị: simulation_id={simulation_id}")
    
    # Cờ đánh dấu để tránh gọi `stop_all` lặp lại
    _stop_all_done = False
    
    @classmethod
    def stop_all(cls):
        """Dừng tất cả updater."""
        # Tránh gọi lặp lại
        if cls._stop_all_done:
            return
        cls._stop_all_done = True
        
        with cls._lock:
            if cls._updaters:
                for simulation_id, updater in list(cls._updaters.items()):
                    try:
                        updater.stop()
                    except Exception as e:
                        logger.error(f"Dừng updater thất bại: simulation_id={simulation_id}, error={e}")
                cls._updaters.clear()
            logger.info("Đã dừng toàn bộ updater bộ nhớ đồ thị")
    
    @classmethod
    def get_all_stats(cls) -> Dict[str, Dict[str, Any]]:
        """Lấy thông tin thống kê của tất cả updater."""
        return {
            sim_id: updater.get_stats() 
            for sim_id, updater in cls._updaters.items()
        }
