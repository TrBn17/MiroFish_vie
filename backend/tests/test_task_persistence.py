import os

import pytest


def test_task_persists_across_restart(tmp_path, monkeypatch):
    # Import inside the test so monkeypatching happens before TaskManager singleton is created.
    from app.models import task as task_module

    # Force the task storage into a temp uploads folder.
    monkeypatch.setattr(task_module.Config, "UPLOAD_FOLDER", str(tmp_path / "uploads"), raising=False)

    # Reset singleton so it re-reads Config.UPLOAD_FOLDER.
    task_module.TaskManager._instance = None

    manager = task_module.TaskManager()
    task_id = manager.create_task("unit-test", metadata={"x": 1})
    manager.update_task(task_id, status=task_module.TaskStatus.PROCESSING, progress=33, message="ok")

    storage_path = os.path.join(task_module.Config.UPLOAD_FOLDER, "tasks", "tasks.json")
    assert os.path.exists(storage_path)

    # Simulate a process restart.
    task_module.TaskManager._instance = None
    manager2 = task_module.TaskManager()

    loaded = manager2.get_task(task_id)
    assert loaded is not None
    assert loaded.task_id == task_id
    assert loaded.status == task_module.TaskStatus.PROCESSING
    assert loaded.progress == 33
    assert loaded.metadata.get("x") == 1
