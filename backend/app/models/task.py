"""Task state management for long-running jobs such as graph builds.

Tasks are used by multiple async/background flows (graph building, simulation preparation,
report generation). Historically they were stored only in memory, which meant a backend
restart made existing task IDs unqueryable ("task not found").

This module persists tasks to a small JSON file under the uploads directory so task
status can be retrieved across restarts.
"""

import json
import os
import uuid
import threading
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

from ..config import Config


class TaskStatus(str, Enum):
    """Task status enum."""
    PENDING = "pending"          # Waiting
    PROCESSING = "processing"    # Processing
    COMPLETED = "completed"      # Completed
    FAILED = "failed"            # Failed


@dataclass
class Task:
    """Task data class."""
    task_id: str
    task_type: str
    status: TaskStatus
    created_at: datetime
    updated_at: datetime
    progress: int = 0              # Overall progress percentage, 0-100
    message: str = ""              # Status message
    result: Optional[Dict] = None  # Task result
    error: Optional[str] = None    # Error details
    metadata: Dict = field(default_factory=dict)  # Extra metadata
    progress_detail: Dict = field(default_factory=dict)  # Detailed progress info
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to a dictionary."""
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "progress": self.progress,
            "message": self.message,
            "progress_detail": self.progress_detail,
            "result": self.result,
            "error": self.error,
            "metadata": self.metadata,
        }


class TaskManager:
    """Thread-safe task manager."""
    _tasks: Dict[str, Task]
    _task_lock: threading.Lock
    _storage_path: str
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton constructor."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._tasks = {}
                    cls._instance._task_lock = threading.Lock()
                    cls._instance._storage_path = os.path.join(Config.UPLOAD_FOLDER, 'tasks', 'tasks.json')
                    cls._instance._load_from_disk()
        return cls._instance

    def _ensure_storage_dir(self) -> None:
        storage_dir = os.path.dirname(self._storage_path)
        if storage_dir:
            os.makedirs(storage_dir, exist_ok=True)

    def _persist_locked(self) -> None:
        """Persist tasks to disk.

        Must be called with `_task_lock` held.
        """
        self._ensure_storage_dir()

        payload = {
            "version": 1,
            "saved_at": datetime.now().isoformat(),
            "tasks": [task.to_dict() for task in self._tasks.values()],
        }

        tmp_path = f"{self._storage_path}.tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self._storage_path)

    def _load_from_disk(self) -> None:
        """Load tasks from disk if present.

        Best-effort: ignores invalid entries.
        """
        try:
            if not os.path.exists(self._storage_path):
                return

            with open(self._storage_path, 'r', encoding='utf-8') as f:
                raw = json.load(f)

            tasks: List[dict] = []
            if isinstance(raw, dict):
                tasks = raw.get('tasks', []) if isinstance(raw.get('tasks', []), list) else []
            elif isinstance(raw, list):
                # Backward compatibility if a plain list was written.
                tasks = raw

            loaded: Dict[str, Task] = {}
            for item in tasks:
                if not isinstance(item, dict):
                    continue
                task_id = item.get('task_id')
                if not task_id or not isinstance(task_id, str):
                    continue

                try:
                    created_at = datetime.fromisoformat(item.get('created_at')) if item.get('created_at') else datetime.now()
                    updated_at = datetime.fromisoformat(item.get('updated_at')) if item.get('updated_at') else created_at
                except Exception:
                    created_at = datetime.now()
                    updated_at = created_at

                status_val = item.get('status', TaskStatus.PENDING.value)
                try:
                    status = TaskStatus(status_val)
                except Exception:
                    status = TaskStatus.PENDING

                loaded[task_id] = Task(
                    task_id=task_id,
                    task_type=item.get('task_type', ''),
                    status=status,
                    created_at=created_at,
                    updated_at=updated_at,
                    progress=int(item.get('progress', 0) or 0),
                    message=item.get('message', '') or '',
                    result=item.get('result'),
                    error=item.get('error'),
                    metadata=item.get('metadata') or {},
                    progress_detail=item.get('progress_detail') or {},
                )

            with self._task_lock:
                # Do not overwrite tasks that may have been added before load finished.
                self._tasks.update(loaded)

        except Exception:
            # Never crash app initialization due to task persistence.
            return
    
    def create_task(self, task_type: str, metadata: Optional[Dict] = None) -> str:
        """
        Create a new task.

        Args:
            task_type: Task type.
            metadata: Extra metadata.

        Returns:
            Task ID.
        """
        task_id = str(uuid.uuid4())
        now = datetime.now()
        
        task = Task(
            task_id=task_id,
            task_type=task_type,
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
            metadata=metadata or {}
        )
        
        with self._task_lock:
            self._tasks[task_id] = task
            self._persist_locked()
        
        return task_id
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task."""
        with self._task_lock:
            task = self._tasks.get(task_id)
        if task is not None:
            return task

        # Best-effort reload (e.g. after restart, or if another process wrote tasks).
        self._load_from_disk()
        with self._task_lock:
            return self._tasks.get(task_id)
    
    def update_task(
        self,
        task_id: str,
        status: Optional[TaskStatus] = None,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        result: Optional[Dict] = None,
        error: Optional[str] = None,
        progress_detail: Optional[Dict] = None
    ):
        """
        Update task state.

        Args:
            task_id: Task ID.
            status: New status.
            progress: Progress value.
            message: Status message.
            result: Task result.
            error: Error details.
            progress_detail: Detailed progress information.
        """
        with self._task_lock:
            task = self._tasks.get(task_id)
            if task:
                task.updated_at = datetime.now()
                if status is not None:
                    task.status = status
                if progress is not None:
                    task.progress = progress
                if message is not None:
                    task.message = message
                if result is not None:
                    task.result = result
                if error is not None:
                    task.error = error
                if progress_detail is not None:
                    task.progress_detail = progress_detail
                self._persist_locked()
    
    def complete_task(self, task_id: str, result: Dict):
        """Mark a task as completed."""
        self.update_task(
            task_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            message="Task completed",
            result=result
        )
    
    def fail_task(self, task_id: str, error: str):
        """Mark a task as failed."""
        self.update_task(
            task_id,
            status=TaskStatus.FAILED,
            message="Task failed",
            error=error
        )
    
    def list_tasks(self, task_type: Optional[str] = None) -> list:
        """List tasks."""
        with self._task_lock:
            tasks = list(self._tasks.values())
            if task_type:
                tasks = [t for t in tasks if t.task_type == task_type]
            return [t.to_dict() for t in sorted(tasks, key=lambda x: x.created_at, reverse=True)]
    
    def cleanup_old_tasks(self, max_age_hours: int = 24):
        """Clean up old tasks."""
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        
        with self._task_lock:
            old_ids = [
                tid for tid, task in self._tasks.items()
                if task.created_at < cutoff and task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]
            ]
            for tid in old_ids:
                del self._tasks[tid]
            if old_ids:
                self._persist_locked()

