"""Simulation API routes for entity filtering and OASIS simulation setup and execution."""

import os
import traceback
from typing import Any, Dict, List
from flask import request, jsonify, send_file

from . import simulation_bp
from ..config import Config
from ..services.zep_entity_reader import ZepEntityReader
from ..services.oasis_profile_generator import OasisProfileGenerator, normalize_interested_topics
from ..services.simulation_manager import SimulationManager, SimulationStatus
from ..services.simulation_runner import SimulationRunner, RunnerStatus
from ..utils.logger import get_logger
from ..models.project import ProjectManager

logger = get_logger('mirofish.api.simulation')


# Interview prompt prefix
# This prefix nudges the agent to reply directly in text instead of calling tools.
INTERVIEW_PROMPT_PREFIX = "Based on your persona, complete memory, and past actions, reply directly in text without calling any tools: "


def _normalize_profiles_interested_topics(profiles: Any) -> List[Dict[str, Any]]:
    """Normalize topic payloads before returning profile data to the UI."""
    if not isinstance(profiles, list):
        return []

    normalized_profiles: List[Dict[str, Any]] = []
    for profile in profiles:
        if not isinstance(profile, dict):
            continue
        normalized_profile = dict(profile)
        normalized_profile["interested_topics"] = normalize_interested_topics(
            profile.get("interested_topics")
        )
        normalized_profiles.append(normalized_profile)

    return normalized_profiles


def optimize_interview_prompt(prompt: str) -> str:
    """
    Optimize an interview question by adding a prefix that discourages tool use.

    Args:
        prompt: Original question.

    Returns:
        Optimized question.
    """
    if not prompt:
        return prompt
    # Avoid adding the prefix twice.
    if prompt.startswith(INTERVIEW_PROMPT_PREFIX):
        return prompt
    return f"{INTERVIEW_PROMPT_PREFIX}{prompt}"


# ============== Entity retrieval APIs ==============

@simulation_bp.route('/entities/<graph_id>', methods=['GET'])
def get_graph_entities(graph_id: str):
    """
    Lay tat ca thuc the trong do thi (da loc)
    
    Chi tra ve cac nut phu hop voi loai thuc the da dinh nghia san (bao gom ca nhung nut co label khong chi la Entity)
    
    Tham so truy van:
        entity_types: Danh sach loai thuc the tach bang dau phay (tuy chon, dung de loc them)
        enrich: Co lay thong tin canh lien quan hay khong (mac dinh la true)
    """
    try:
        if not Config.ZEP_API_KEY:
            return jsonify({
                "success": False,
                "error": "ZEP_API_KEY is not configured"
            }), 500
        
        entity_types_str = request.args.get('entity_types', '')
        entity_types = [t.strip() for t in entity_types_str.split(',') if t.strip()] if entity_types_str else None
        enrich = request.args.get('enrich', 'true').lower() == 'true'
        
        logger.info(f"Fetching graph entities: graph_id={graph_id}, entity_types={entity_types}, enrich={enrich}")
        
        reader = ZepEntityReader()
        result = reader.filter_defined_entities(
            graph_id=graph_id,
            defined_entity_types=entity_types,
            enrich_with_edges=enrich
        )
        
        return jsonify({
            "success": True,
            "data": result.to_dict()
        })
        
    except Exception as e:
        logger.error(f"Failed to fetch graph entities: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/entities/<graph_id>/<entity_uuid>', methods=['GET'])
def get_entity_detail(graph_id: str, entity_uuid: str):
    """Get detailed information for a single entity."""
    try:
        if not Config.ZEP_API_KEY:
            return jsonify({
                "success": False,
                "error": "ZEP_API_KEY is not configured"
            }), 500
        
        reader = ZepEntityReader()
        entity = reader.get_entity_with_context(graph_id, entity_uuid)
        
        if not entity:
            return jsonify({
                "success": False,
                "error": f"Entity not found: {entity_uuid}"
            }), 404
        
        return jsonify({
            "success": True,
            "data": entity.to_dict()
        })
        
    except Exception as e:
        logger.error(f"Failed to fetch entity details: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/entities/<graph_id>/by-type/<entity_type>', methods=['GET'])
def get_entities_by_type(graph_id: str, entity_type: str):
    """Get all entities of the specified type."""
    try:
        if not Config.ZEP_API_KEY:
            return jsonify({
                "success": False,
                "error": "ZEP_API_KEY is not configured"
            }), 500
        
        enrich = request.args.get('enrich', 'true').lower() == 'true'
        
        reader = ZepEntityReader()
        entities = reader.get_entities_by_type(
            graph_id=graph_id,
            entity_type=entity_type,
            enrich_with_edges=enrich
        )
        
        return jsonify({
            "success": True,
            "data": {
                "entity_type": entity_type,
                "count": len(entities),
                "entities": [e.to_dict() for e in entities]
            }
        })
        
    except Exception as e:
        logger.error(f"Failed to fetch entities: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== Simulation management APIs ==============

@simulation_bp.route('/create', methods=['POST'])
def create_simulation():
    """
    Tao mo phong moi
    
    Luu y: cac tham so nhu max_rounds duoc LLM sinh tu dong, khong can cai dat thu cong
    
    Yeu cau (JSON):
        {
            "project_id": "proj_xxxx",      // bat buoc
            "graph_id": "mirofish_xxxx",    // tuy chon, neu khong cung cap se lay tu project
            "enable_twitter": true,          // tuy chon, mac dinh la true
            "enable_reddit": true            // tuy chon, mac dinh la true
        }
    
    Tra ve:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "project_id": "proj_xxxx",
                "graph_id": "mirofish_xxxx",
                "status": "created",
                "enable_twitter": true,
                "enable_reddit": true,
                "created_at": "2025-12-01T10:00:00"
            }
        }
    """
    try:
        data = request.get_json() or {}
        
        project_id = data.get('project_id')
        if not project_id:
            return jsonify({
                "success": False,
                "error": "Please provide project_id"
            }), 400
        
        project = ProjectManager.get_project(project_id)
        if not project:
            return jsonify({
                "success": False,
                "error": f"Project not found: {project_id}"
            }), 404
        
        graph_id = data.get('graph_id') or project.graph_id
        if not graph_id:
            return jsonify({
                "success": False,
                "error": "The project does not have a graph yet. Please call /api/graph/build first"
            }), 400
        
        manager = SimulationManager()
        state = manager.create_simulation(
            project_id=project_id,
            graph_id=graph_id,
            enable_twitter=data.get('enable_twitter', True),
            enable_reddit=data.get('enable_reddit', True),
        )
        
        return jsonify({
            "success": True,
            "data": state.to_dict()
        })
        
    except Exception as e:
        logger.error(f"Failed to create simulation: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


def _check_simulation_prepared(simulation_id: str) -> tuple:
    """
    Kiem tra xem mo phong da duoc chuan bi xong hay chua
    
    Dieu kien kiem tra:
    1. state.json ton tai va status la "ready"
    2. Cac tep bat buoc ton tai: reddit_profiles.json, twitter_profiles.csv, simulation_config.json
    
    Luu y: cac script chay (run_*.py) duoc giu trong thu muc backend/scripts/ va khong con duoc sao chep vao thu muc mo phong
    
    Args:
        simulation_id: ID mo phong
        
    Returns:
        (is_prepared: bool, info: dict)
    """
    import os
    from ..config import Config
    
    simulation_dir = os.path.join(Config.OASIS_SIMULATION_DATA_DIR, simulation_id)
    
    # Kiem tra thu muc co ton tai hay khong
    if not os.path.exists(simulation_dir):
        return False, {"reason": "Thu muc mo phong khong ton tai"}
    
    # Danh sach tep bat buoc (khong bao gom script, vi script nam trong backend/scripts/)
    required_files = [
        "state.json",
        "simulation_config.json",
        "reddit_profiles.json",
        "twitter_profiles.csv"
    ]
    
    # Kiem tra tep co ton tai hay khong
    existing_files = []
    missing_files = []
    for f in required_files:
        file_path = os.path.join(simulation_dir, f)
        if os.path.exists(file_path):
            existing_files.append(f)
        else:
            missing_files.append(f)
    
    if missing_files:
        return False, {
            "reason": "Thieu tep bat buoc",
            "missing_files": missing_files,
            "existing_files": existing_files
        }
    
    # Kiem tra trang thai trong state.json
    state_file = os.path.join(simulation_dir, "state.json")
    try:
        import json
        with open(state_file, 'r', encoding='utf-8') as f:
            state_data = json.load(f)
        
        status = state_data.get("status", "")
        config_generated = state_data.get("config_generated", False)
        
        # Nhat ky chi tiet
        logger.debug(f"Kiem tra trang thai chuan bi mo phong: {simulation_id}, status={status}, config_generated={config_generated}")
        
        # Neu config_generated=True va cac tep ton tai, xem nhu da chuan bi xong
        # Cac trang thai sau deu cho thay viec chuan bi da hoan tat:
        # - ready: da chuan bi xong, co the chay
        # - preparing: neu config_generated=True thi nghia la da xong
        # - running: dang chay, tuc la da chuan bi xong tu truoc
        # - completed: da chay xong, tuc la da chuan bi xong tu truoc
        # - stopped: da dung, tuc la da chuan bi xong tu truoc
        # - failed: chay that bai (nhung phan chuan bi da hoan tat)
        prepared_statuses = ["ready", "preparing", "running", "completed", "stopped", "failed"]
        if status in prepared_statuses and config_generated:
            # Lay thong tin thong ke tep
            profiles_file = os.path.join(simulation_dir, "reddit_profiles.json")
            config_file = os.path.join(simulation_dir, "simulation_config.json")
            
            profiles_count = 0
            if os.path.exists(profiles_file):
                with open(profiles_file, 'r', encoding='utf-8') as f:
                    profiles_data = json.load(f)
                    profiles_count = len(profiles_data) if isinstance(profiles_data, list) else 0
            
            # Neu trang thai la preparing nhung cac tep da hoan tat, tu dong cap nhat sang ready
            if status == "preparing":
                try:
                    state_data["status"] = "ready"
                    from datetime import datetime
                    state_data["updated_at"] = datetime.now().isoformat()
                    with open(state_file, 'w', encoding='utf-8') as f:
                        json.dump(state_data, f, ensure_ascii=False, indent=2)
                    logger.info(f"Tu dong cap nhat trang thai mo phong: {simulation_id} preparing -> ready")
                    status = "ready"
                except Exception as e:
                    logger.warning(f"Tu dong cap nhat trang thai that bai: {e}")
            
            logger.info(f"Mo phong {simulation_id} - ket qua kiem tra: da chuan bi xong (status={status}, config_generated={config_generated})")
            return True, {
                "status": status,
                "entities_count": state_data.get("entities_count", 0),
                "profiles_count": profiles_count,
                "entity_types": state_data.get("entity_types", []),
                "config_generated": config_generated,
                "created_at": state_data.get("created_at"),
                "updated_at": state_data.get("updated_at"),
                "existing_files": existing_files
            }
        else:
            logger.warning(f"Mo phong {simulation_id} - ket qua kiem tra: chua chuan bi xong (status={status}, config_generated={config_generated})")
            return False, {
                "reason": f"Trang thai khong nam trong danh sach da chuan bi hoac config_generated la false: status={status}, config_generated={config_generated}",
                "status": status,
                "config_generated": config_generated
            }
            
    except Exception as e:
        return False, {"reason": f"Doc tep trang thai that bai: {str(e)}"}


@simulation_bp.route('/prepare', methods=['POST'])
def prepare_simulation():
    """
    Chuan bi moi truong mo phong (tac vu bat dong bo, LLM sinh tu dong moi tham so)
    
    Day la thao tac ton thoi gian, API se tra ve task_id ngay lap tuc,
    Dung GET /api/simulation/prepare/status de xem tien do
    
    Tinh nang:
    - Tu dong phat hien phan chuan bi da hoan tat de tranh sinh lai
    - Neu da chuan bi xong, tra ve ket qua san co ngay
    - Ho tro bat buoc sinh lai (force_regenerate=true)
    
    Cac buoc:
    1. Kiem tra xem da co phan chuan bi hoan tat hay chua
    2. Doc va loc thuc the tu do thi Zep
    3. Sinh OASIS Agent Profile cho tung thuc the (co co che thu lai)
    4. LLM tu dong sinh cau hinh mo phong (co co che thu lai)
    5. Luu tep cau hinh va script san
    
    Yeu cau (JSON):
        {
            "simulation_id": "sim_xxxx",                   // bat buoc, ID mo phong
            "entity_types": ["Student", "PublicFigure"],  // tuy chon, chi dinh loai thuc the
            "use_llm_for_profiles": true,                 // tuy chon, co dung LLM de sinh ho so nhan vat hay khong
            "parallel_profile_count": 5,                  // tuy chon, so luong ho so nhan vat sinh song song, mac dinh la 5
            "force_regenerate": false                     // tuy chon, bat buoc sinh lai, mac dinh la false
        }
    
    Tra ve:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "task_id": "task_xxxx",           // tra ve khi la tac vu moi
                "status": "preparing|ready",
                "message": "Tac vu chuan bi da duoc khoi dong|Da co phan chuan bi hoan tat",
                "already_prepared": true|false    // da chuan bi xong hay chua
            }
        }
    """
    import threading
    import os
    from ..models.task import TaskManager, TaskStatus
    from ..config import Config
    
    try:
        data = request.get_json() or {}
        
        simulation_id = data.get('simulation_id')
        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap simulation_id"
            }), 400
        
        manager = SimulationManager()
        state = manager.get_simulation(simulation_id)
        
        if not state:
            return jsonify({
                "success": False,
                "error": f"Mo phong khong ton tai: {simulation_id}"
            }), 404
        
        # Kiem tra co bat buoc sinh lai hay khong
        force_regenerate = data.get('force_regenerate', False)
        logger.info(f"Bat dau xu ly yeu cau /prepare: simulation_id={simulation_id}, force_regenerate={force_regenerate}")
        
        # Kiem tra xem da chuan bi xong hay chua (tranh sinh lai)
        if not force_regenerate:
            logger.debug(f"Kiem tra mo phong {simulation_id} da chuan bi xong hay chua...")
            is_prepared, prepare_info = _check_simulation_prepared(simulation_id)
            logger.debug(f"Ket qua kiem tra: is_prepared={is_prepared}, prepare_info={prepare_info}")
            if is_prepared:
                logger.info(f"Mo phong {simulation_id} da chuan bi xong, bo qua viec sinh lap lai")
                return jsonify({
                    "success": True,
                    "data": {
                        "simulation_id": simulation_id,
                        "status": "ready",
                        "message": "Da co phan chuan bi hoan tat, khong can sinh lai",
                        "already_prepared": True,
                        "prepare_info": prepare_info
                    }
                })
            else:
                logger.info(f"Mo phong {simulation_id} chua chuan bi xong, se khoi dong tac vu chuan bi")
        
        # Lay thong tin can thiet tu project
        project = ProjectManager.get_project(state.project_id)
        if not project:
            return jsonify({
                "success": False,
                "error": f"Project khong ton tai: {state.project_id}"
            }), 404
        
        # Lay yeu cau mo phong
        simulation_requirement = project.simulation_requirement or ""
        if not simulation_requirement:
            return jsonify({
                "success": False,
                "error": "Project thieu mo ta yeu cau mo phong (simulation_requirement)"
            }), 400
        
        # Lay noi dung van ban tai lieu
        document_text = ProjectManager.get_extracted_text(state.project_id) or ""
        
        entity_types_list = data.get('entity_types')
        use_llm_for_profiles = data.get('use_llm_for_profiles', True)
        parallel_profile_count = data.get('parallel_profile_count', 5)
        
        # ========== Lay dong bo so luong thuc the (truoc khi tac vu nen bat dau) ==========
        # Nho vay frontend co the lay ngay tong so Agent du kien sau khi goi prepare
        try:
            logger.info(f"Lay dong bo so luong thuc the: graph_id={state.graph_id}")
            reader = ZepEntityReader()
            # Doc nhanh thuc the (khong can thong tin canh, chi dem so luong)
            filtered_preview = reader.filter_defined_entities(
                graph_id=state.graph_id,
                defined_entity_types=entity_types_list,
                enrich_with_edges=False  # Khong lay thong tin canh de tang toc
            )
            # Luu so luong thuc the vao trang thai (de frontend lay ngay)
            state.entities_count = filtered_preview.filtered_count
            state.entity_types = list(filtered_preview.entity_types)
            logger.info(f"So luong thuc the du kien: {filtered_preview.filtered_count}, loai: {filtered_preview.entity_types}")
        except Exception as e:
            logger.warning(f"Lay dong bo so luong thuc the that bai (se thu lai trong tac vu nen): {e}")
            # Loi nay khong anh huong den quy trinh tiep theo, tac vu nen se lay lai
        
        # Tao tac vu bat dong bo
        task_manager = TaskManager()
        task_id = task_manager.create_task(
            task_type="simulation_prepare",
            metadata={
                "simulation_id": simulation_id,
                "project_id": state.project_id
            }
        )
        
        # Cap nhat trang thai mo phong (bao gom so luong thuc the da lay truoc)
        state.status = SimulationStatus.PREPARING
        manager._save_simulation_state(state)
        
        # Dinh nghia tac vu nen
        def run_prepare():
            try:
                task_manager.update_task(
                    task_id,
                    status=TaskStatus.PROCESSING,
                    progress=0,
                    message="Bat dau chuan bi moi truong mo phong..."
                )
                
                # Chuan bi mo phong (co callback tien do)
                # Luu chi tiet tien do tung giai doan
                stage_details = {}
                
                def progress_callback(stage, progress, message, **kwargs):
                    # Tinh tong tien do
                    stage_weights = {
                        "reading": (0, 20),           # 0-20%
                        "generating_profiles": (20, 70),  # 20-70%
                        "generating_config": (70, 90),    # 70-90%
                        "copying_scripts": (90, 100)       # 90-100%
                    }
                    
                    start, end = stage_weights.get(stage, (0, 100))
                    current_progress = int(start + (end - start) * progress / 100)
                    
                    # Tao thong tin tien do chi tiet
                    stage_names = {
                        "reading": "Doc thuc the tu do thi",
                        "generating_profiles": "Sinh ho so Agent",
                        "generating_config": "Sinh cau hinh mo phong",
                        "copying_scripts": "Chuan bi script mo phong"
                    }
                    
                    stage_index = list(stage_weights.keys()).index(stage) + 1 if stage in stage_weights else 1
                    total_stages = len(stage_weights)
                    
                    # Cap nhat chi tiet giai doan
                    stage_details[stage] = {
                        "stage_name": stage_names.get(stage, stage),
                        "stage_progress": progress,
                        "current": kwargs.get("current", 0),
                        "total": kwargs.get("total", 0),
                        "item_name": kwargs.get("item_name", "")
                    }
                    
                    # Tao thong tin tien do chi tiet
                    detail = stage_details[stage]
                    progress_detail_data = {
                        "current_stage": stage,
                        "current_stage_name": stage_names.get(stage, stage),
                        "stage_index": stage_index,
                        "total_stages": total_stages,
                        "stage_progress": progress,
                        "current_item": detail["current"],
                        "total_items": detail["total"],
                        "item_description": message
                    }
                    
                    # Tao thong diep ngan gon
                    if detail["total"] > 0:
                        detailed_message = (
                            f"[{stage_index}/{total_stages}] {stage_names.get(stage, stage)}: "
                            f"{detail['current']}/{detail['total']} - {message}"
                        )
                    else:
                        detailed_message = f"[{stage_index}/{total_stages}] {stage_names.get(stage, stage)}: {message}"
                    
                    task_manager.update_task(
                        task_id,
                        progress=current_progress,
                        message=detailed_message,
                        progress_detail=progress_detail_data
                    )
                
                result_state = manager.prepare_simulation(
                    simulation_id=simulation_id,
                    simulation_requirement=simulation_requirement,
                    document_text=document_text,
                    defined_entity_types=entity_types_list,
                    use_llm_for_profiles=use_llm_for_profiles,
                    progress_callback=progress_callback,
                    parallel_profile_count=parallel_profile_count
                )
                
                # Tac vu hoan tat
                task_manager.complete_task(
                    task_id,
                    result=result_state.to_simple_dict()
                )
                
            except Exception as e:
                logger.error(f"Chuan bi mo phong that bai: {str(e)}")
                task_manager.fail_task(task_id, str(e))
                
                # Cap nhat trang thai mo phong thanh that bai
                state = manager.get_simulation(simulation_id)
                if state:
                    state.status = SimulationStatus.FAILED
                    state.error = str(e)
                    manager._save_simulation_state(state)
        
        # Khoi dong luong nen
        thread = threading.Thread(target=run_prepare, daemon=True)
        thread.start()
        
        return jsonify({
            "success": True,
            "data": {
                "simulation_id": simulation_id,
                "task_id": task_id,
                "status": "preparing",
                "message": "Tac vu chuan bi da duoc khoi dong, vui long dung /api/simulation/prepare/status de xem tien do",
                "already_prepared": False,
                "expected_entities_count": state.entities_count,  # Tong so Agent du kien
                "entity_types": state.entity_types  # Danh sach loai thuc the
            }
        })
        
    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 404
        
    except Exception as e:
        logger.error(f"Khoi dong tac vu chuan bi that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/prepare/status', methods=['POST'])
def get_prepare_status():
    """
    Truy van tien do tac vu chuan bi
    
    Ho tro hai cach truy van:
    1. Truy van tien do tac vu dang chay bang task_id
    2. Kiem tra bang simulation_id xem da co phan chuan bi hoan tat hay chua
    
    Yeu cau (JSON):
        {
            "task_id": "task_xxxx",          // tuy chon, task_id duoc prepare tra ve
            "simulation_id": "sim_xxxx"      // tuy chon, ID mo phong (de kiem tra phan chuan bi da hoan tat)
        }
    
    Tra ve:
        {
            "success": true,
            "data": {
                "task_id": "task_xxxx",
                "status": "processing|completed|ready",
                "progress": 45,
                "message": "...",
                "already_prepared": true|false,  // da co phan chuan bi hoan tat hay chua
                "prepare_info": {...}            // Thong tin chi tiet khi da chuan bi xong
            }
        }
    """
    from ..models.task import TaskManager
    
    try:
        data = request.get_json() or {}
        
        task_id = data.get('task_id')
        simulation_id = data.get('simulation_id')
        
        # Neu co cung cap simulation_id, truoc tien kiem tra xem da chuan bi xong hay chua
        if simulation_id:
            is_prepared, prepare_info = _check_simulation_prepared(simulation_id)
            if is_prepared:
                return jsonify({
                    "success": True,
                    "data": {
                        "simulation_id": simulation_id,
                        "status": "ready",
                        "progress": 100,
                        "message": "Da co phan chuan bi hoan tat",
                        "already_prepared": True,
                        "prepare_info": prepare_info
                    }
                })
        
        # Neu khong co task_id, tra ve loi
        if not task_id:
            if simulation_id:
                # Co simulation_id nhung chua chuan bi xong
                return jsonify({
                    "success": True,
                    "data": {
                        "simulation_id": simulation_id,
                        "status": "not_started",
                        "progress": 0,
                        "message": "Chua bat dau chuan bi, vui long goi /api/simulation/prepare de bat dau",
                        "already_prepared": False
                    }
                })
            return jsonify({
                "success": False,
                "error": "Vui long cung cap task_id hoac simulation_id"
            }), 400
        
        task_manager = TaskManager()
        task = task_manager.get_task(task_id)
        
        if not task:
            # Tac vu khong ton tai, nhung neu co simulation_id thi hay kiem tra xem da chuan bi xong hay chua
            if simulation_id:
                is_prepared, prepare_info = _check_simulation_prepared(simulation_id)
                if is_prepared:
                    return jsonify({
                        "success": True,
                        "data": {
                            "simulation_id": simulation_id,
                            "task_id": task_id,
                            "status": "ready",
                            "progress": 100,
                            "message": "Tac vu da hoan tat (phan chuan bi da ton tai)",
                            "already_prepared": True,
                            "prepare_info": prepare_info
                        }
                    })
            
            return jsonify({
                "success": False,
                "error": f"Tac vu khong ton tai: {task_id}"
            }), 404
        
        task_dict = task.to_dict()
        task_dict["already_prepared"] = False
        
        return jsonify({
            "success": True,
            "data": task_dict
        })
        
    except Exception as e:
        logger.error(f"Truy van trang thai tac vu that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@simulation_bp.route('/<simulation_id>', methods=['GET'])
def get_simulation(simulation_id: str):
    """Lay trang thai mo phong"""
    try:
        manager = SimulationManager()
        state = manager.get_simulation(simulation_id)
        
        if not state:
            return jsonify({
                "success": False,
                "error": f"Mo phong khong ton tai: {simulation_id}"
            }), 404
        
        result = state.to_dict()
        
        # Neu mo phong da san sang, bo sung huong dan chay
        if state.status == SimulationStatus.READY:
            result["run_instructions"] = manager.get_run_instructions(simulation_id)
        
        return jsonify({
            "success": True,
            "data": result
        })
        
    except Exception as e:
        logger.error(f"Lay trang thai mo phong that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/list', methods=['GET'])
def list_simulations():
    """
    Liet ke tat ca mo phong
    
    Tham so truy van:
        project_id: Loc theo ID project (tuy chon)
    """
    try:
        project_id = request.args.get('project_id')
        
        manager = SimulationManager()
        simulations = manager.list_simulations(project_id=project_id)
        
        return jsonify({
            "success": True,
            "data": [s.to_dict() for s in simulations],
            "count": len(simulations)
        })
        
    except Exception as e:
        logger.error(f"Liet ke mo phong that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


def _get_report_id_for_simulation(simulation_id: str) -> str:
    """
    Lay report_id moi nhat ung voi simulation
    
    Duyet thu muc reports de tim report co simulation_id khop,
    neu co nhieu ban ghi thi tra ve ban moi nhat (sap xep theo created_at)
    
    Args:
        simulation_id: ID mo phong
        
    Returns:
        report_id hoac None
    """
    import json
    from datetime import datetime
    
    # Duong dan thu muc reports: backend/uploads/reports
    # __file__ la app/api/simulation.py, can di nguoc len hai cap de toi backend/
    reports_dir = os.path.join(os.path.dirname(__file__), '../../uploads/reports')
    if not os.path.exists(reports_dir):
        return None
    
    matching_reports = []
    
    try:
        for report_folder in os.listdir(reports_dir):
            report_path = os.path.join(reports_dir, report_folder)
            if not os.path.isdir(report_path):
                continue
            
            meta_file = os.path.join(report_path, "meta.json")
            if not os.path.exists(meta_file):
                continue
            
            try:
                with open(meta_file, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                
                if meta.get("simulation_id") == simulation_id:
                    matching_reports.append({
                        "report_id": meta.get("report_id"),
                        "created_at": meta.get("created_at", ""),
                        "status": meta.get("status", "")
                    })
            except Exception:
                continue
        
        if not matching_reports:
            return None
        
        # Sap xep giam dan theo thoi gian tao va tra ve ban moi nhat
        matching_reports.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return matching_reports[0].get("report_id")
        
    except Exception as e:
        logger.warning(f"Tim report cho simulation {simulation_id} that bai: {e}")
        return None


@simulation_bp.route('/history', methods=['GET'])
def get_simulation_history():
    """
    Lay danh sach mo phong lich su (kem chi tiet project)
    
    Dung de hien thi lich su project tren trang chu, tra ve danh sach mo phong gom ten project, mo ta va cac thong tin phong phu khac
    
    Tham so truy van:
        limit: Gioi han so luong tra ve (mac dinh la 20)
    
    Tra ve:
        {
            "success": true,
            "data": [
                {
                    "simulation_id": "sim_xxxx",
                    "project_id": "proj_xxxx",
                    "project_name": "Phan tich du luan DH Vu Han",
                    "simulation_requirement": "Neu Dai hoc Vu Han dang tai...",
                    "status": "completed",
                    "entities_count": 68,
                    "profiles_count": 68,
                    "entity_types": ["Student", "Professor", ...],
                    "created_at": "2024-12-10",
                    "updated_at": "2024-12-10",
                    "total_rounds": 120,
                    "current_round": 120,
                    "report_id": "report_xxxx",
                    "version": "v1.0.2"
                },
                ...
            ],
            "count": 7
        }
    """
    try:
        limit = request.args.get('limit', 20, type=int)
        
        manager = SimulationManager()
        simulations = manager.list_simulations()[:limit]
        
        # Bo sung du lieu mo phong, chi doc tu tep Simulation
        enriched_simulations = []
        for sim in simulations:
            sim_dict = sim.to_dict()
            
            # Lay thong tin cau hinh mo phong (doc simulation_requirement tu simulation_config.json)
            config = manager.get_simulation_config(sim.simulation_id)
            if config:
                sim_dict["simulation_requirement"] = config.get("simulation_requirement", "")
                time_config = config.get("time_config", {})
                sim_dict["total_simulation_hours"] = time_config.get("total_simulation_hours", 0)
                # So vong de xuat (gia tri du phong)
                recommended_rounds = int(
                    time_config.get("total_simulation_hours", 0) * 60 / 
                    max(time_config.get("minutes_per_round", 60), 1)
                )
            else:
                sim_dict["simulation_requirement"] = ""
                sim_dict["total_simulation_hours"] = 0
                recommended_rounds = 0
            
            # Lay trang thai chay (doc so vong thuc te do nguoi dung dat trong run_state.json)
            run_state = SimulationRunner.get_run_state(sim.simulation_id)
            if run_state:
                sim_dict["current_round"] = run_state.current_round
                sim_dict["runner_status"] = run_state.runner_status.value
                # Su dung total_rounds do nguoi dung dat, neu khong co thi dung so vong de xuat
                sim_dict["total_rounds"] = run_state.total_rounds if run_state.total_rounds > 0 else recommended_rounds
            else:
                sim_dict["current_round"] = 0
                sim_dict["runner_status"] = "idle"
                sim_dict["total_rounds"] = recommended_rounds
            
            # Lay danh sach tep cua project lien ket (toi da 3 tep)
            project = ProjectManager.get_project(sim.project_id)
            if project and hasattr(project, 'files') and project.files:
                sim_dict["files"] = [
                    {"filename": f.get("filename", "Tep khong xac dinh")} 
                    for f in project.files[:3]
                ]
            else:
                sim_dict["files"] = []
            
            # Lay report_id lien ket (tim report moi nhat cua simulation nay)
            sim_dict["report_id"] = _get_report_id_for_simulation(sim.simulation_id)
            
            # Them so phien ban
            sim_dict["version"] = "v1.0.2"
            
            # Dinh dang ngay thang
            try:
                created_date = sim_dict.get("created_at", "")[:10]
                sim_dict["created_date"] = created_date
            except:
                sim_dict["created_date"] = ""
            
            enriched_simulations.append(sim_dict)
        
        return jsonify({
            "success": True,
            "data": enriched_simulations,
            "count": len(enriched_simulations)
        })
        
    except Exception as e:
        logger.error(f"Lay lich su mo phong that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/<simulation_id>/profiles', methods=['GET'])
def get_simulation_profiles(simulation_id: str):
    """
    Lay Agent Profile cua mo phong
    
    Tham so truy van:
        platform: Loai nen tang (reddit/twitter, mac dinh la reddit)
    """
    try:
        platform = request.args.get('platform', 'reddit')
        
        manager = SimulationManager()
        profiles = _normalize_profiles_interested_topics(
            manager.get_profiles(simulation_id, platform=platform)
        )
        
        return jsonify({
            "success": True,
            "data": {
                "platform": platform,
                "count": len(profiles),
                "profiles": profiles
            }
        })
        
    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 404
        
    except Exception as e:
        logger.error(f"Lay Profile that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/<simulation_id>/profiles/realtime', methods=['GET'])
def get_simulation_profiles_realtime(simulation_id: str):
    """
    Lay Agent Profile cua mo phong theo thoi gian thuc (de xem tien do trong luc dang sinh)
    
    Khac biet so voi API /profiles:
    - Doc tep truc tiep, khong thong qua SimulationManager
    - Phu hop cho viec xem theo thoi gian thuc trong qua trinh sinh
    - Tra ve them metadata (vi du: thoi gian sua tep, co dang sinh hay khong, ...)
    
    Tham so truy van:
        platform: Loai nen tang (reddit/twitter, mac dinh la reddit)
    
    Tra ve:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "platform": "reddit",
                "count": 15,
                "total_expected": 93,  // Tong so du kien (neu co)
                "is_generating": true,  // co dang sinh hay khong
                "file_exists": true,
                "file_modified_at": "2025-12-04T18:20:00",
                "profiles": [...]
            }
        }
    """
    import json
    import csv
    import time
    from datetime import datetime
    
    try:
        platform = request.args.get('platform', 'reddit')
        
        # Lay thu muc mo phong
        sim_dir = os.path.join(Config.OASIS_SIMULATION_DATA_DIR, simulation_id)
        
        if not os.path.exists(sim_dir):
            return jsonify({
                "success": False,
                "error": f"Mo phong khong ton tai: {simulation_id}"
            }), 404
        
        # Xac dinh duong dan tep
        if platform == "reddit":
            profiles_file = os.path.join(sim_dir, "reddit_profiles.json")
        else:
            profiles_file = os.path.join(sim_dir, "twitter_profiles.csv")
        
        # Kiem tra tep co ton tai hay khong
        file_exists = os.path.exists(profiles_file)
        profiles = []
        file_modified_at = None
        
        if file_exists:
            # Lay thoi gian sua tep
            file_stat = os.stat(profiles_file)
            file_modified_at = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            
            read_error = None
            for attempt in range(3):
                try:
                    if platform == "reddit":
                        with open(profiles_file, 'r', encoding='utf-8') as f:
                            profiles = json.load(f)
                    else:
                        with open(profiles_file, 'r', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            profiles = list(reader)
                    profiles = _normalize_profiles_interested_topics(profiles)
                    read_error = None
                    break
                except (json.JSONDecodeError, Exception) as e:
                    read_error = e
                    if attempt < 2:
                        time.sleep(0.05)
                    else:
                        logger.warning(f"Doc tep profiles that bai (co the dang duoc ghi): {e}")
                        profiles = []
        
        # Kiem tra co dang sinh hay khong (dua tren state.json)
        is_generating = False
        total_expected = None
        
        state_file = os.path.join(sim_dir, "state.json")
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                    status = state_data.get("status", "")
                    is_generating = status == "preparing"
                    total_expected = state_data.get("entities_count")
            except Exception:
                pass
        
        return jsonify({
            "success": True,
            "data": {
                "simulation_id": simulation_id,
                "platform": platform,
                "count": len(profiles),
                "total_expected": total_expected,
                "is_generating": is_generating,
                "file_exists": file_exists,
                "file_modified_at": file_modified_at,
                "profiles": profiles
            }
        })
        
    except Exception as e:
        logger.error(f"Lay Profile theo thoi gian thuc that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/<simulation_id>/config/realtime', methods=['GET'])
def get_simulation_config_realtime(simulation_id: str):
    """
    Lay cau hinh mo phong theo thoi gian thuc (de xem tien do trong luc dang sinh)
    
    Khac biet so voi API /config:
    - Doc tep truc tiep, khong thong qua SimulationManager
    - Phu hop cho viec xem theo thoi gian thuc trong qua trinh sinh
    - Tra ve them metadata (vi du: thoi gian sua tep, co dang sinh hay khong, ...)
    - Ngay ca khi cau hinh chua sinh xong van co the tra ve mot phan thong tin
    
    Tra ve:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "file_exists": true,
                "file_modified_at": "2025-12-04T18:20:00",
                "is_generating": true,  // co dang sinh hay khong
                "generation_stage": "generating_config",  // Giai doan sinh hien tai
                "config": {...}  // Noi dung cau hinh (neu co)
            }
        }
    """
    import json
    from datetime import datetime
    
    try:
        # Lay thu muc mo phong
        sim_dir = os.path.join(Config.OASIS_SIMULATION_DATA_DIR, simulation_id)
        
        if not os.path.exists(sim_dir):
            return jsonify({
                "success": False,
                "error": f"Mo phong khong ton tai: {simulation_id}"
            }), 404
        
        # Duong dan tep cau hinh
        config_file = os.path.join(sim_dir, "simulation_config.json")
        
        # Kiem tra tep co ton tai hay khong
        file_exists = os.path.exists(config_file)
        config = None
        file_modified_at = None
        
        if file_exists:
            # Lay thoi gian sua tep
            file_stat = os.stat(config_file)
            file_modified_at = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            except (json.JSONDecodeError, Exception) as e:
                logger.warning(f"Doc tep config that bai (co the dang duoc ghi): {e}")
                config = None
        
        # Kiem tra co dang sinh hay khong (dua tren state.json)
        is_generating = False
        generation_stage = None
        config_generated = False
        
        state_file = os.path.join(sim_dir, "state.json")
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                    status = state_data.get("status", "")
                    is_generating = status == "preparing"
                    config_generated = state_data.get("config_generated", False)
                    
                    # Xac dinh giai doan hien tai
                    if is_generating:
                        if state_data.get("profiles_generated", False):
                            generation_stage = "generating_config"
                        else:
                            generation_stage = "generating_profiles"
                    elif status == "ready":
                        generation_stage = "completed"
            except Exception:
                pass
        
        # Tao du lieu tra ve
        response_data = {
            "simulation_id": simulation_id,
            "file_exists": file_exists,
            "file_modified_at": file_modified_at,
            "is_generating": is_generating,
            "generation_stage": generation_stage,
            "config_generated": config_generated,
            "config": config
        }
        
        # Neu cau hinh ton tai, trich xuat mot so thong tin thong ke chinh
        if config:
            response_data["summary"] = {
                "total_agents": len(config.get("agent_configs", [])),
                "simulation_hours": config.get("time_config", {}).get("total_simulation_hours"),
                "initial_posts_count": len(config.get("event_config", {}).get("initial_posts", [])),
                "hot_topics_count": len(config.get("event_config", {}).get("hot_topics", [])),
                "has_twitter_config": "twitter_config" in config,
                "has_reddit_config": "reddit_config" in config,
                "generated_at": config.get("generated_at"),
                "llm_model": config.get("llm_model")
            }
        
        return jsonify({
            "success": True,
            "data": response_data
        })
        
    except Exception as e:
        logger.error(f"Lay Config theo thoi gian thuc that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/<simulation_id>/config', methods=['GET'])
def get_simulation_config(simulation_id: str):
    """
    Lay cau hinh mo phong (cau hinh day du do LLM tu dong sinh)
    
    Tra ve bao gom:
        - time_config: Cau hinh thoi gian (thoi luong mo phong, so vong, giai doan cao diem/thap diem)
        - agent_configs: Cau hinh hoat dong cho tung Agent (muc do hoat dong, tan suat phat bieu, lap truong, ...)
        - event_config: Cau hinh su kien (bai dang ban dau, chu de nong)
        - platform_configs: Cau hinh nen tang
        - generation_reasoning: Phan giai thich suy luan cau hinh cua LLM
    """
    try:
        manager = SimulationManager()
        config = manager.get_simulation_config(simulation_id)
        
        if not config:
            return jsonify({
                "success": False,
                "error": f"Cau hinh mo phong khong ton tai, vui long goi API /prepare truoc"
            }), 404
        
        return jsonify({
            "success": True,
            "data": config
        })
        
    except Exception as e:
        logger.error(f"Lay cau hinh that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/<simulation_id>/config/download', methods=['GET'])
def download_simulation_config(simulation_id: str):
    """Tai tep cau hinh mo phong"""
    try:
        manager = SimulationManager()
        sim_dir = manager._get_simulation_dir(simulation_id)
        config_path = os.path.join(sim_dir, "simulation_config.json")
        
        if not os.path.exists(config_path):
            return jsonify({
                "success": False,
                "error": "Tep cau hinh khong ton tai, vui long goi API /prepare truoc"
            }), 404
        
        return send_file(
            config_path,
            as_attachment=True,
            download_name="simulation_config.json"
        )
        
    except Exception as e:
        logger.error(f"Tai cau hinh that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/script/<script_name>/download', methods=['GET'])
def download_simulation_script(script_name: str):
    """
    Tai tep script chay mo phong (script dung chung, nam trong backend/scripts/)
    
    Gia tri hop le cua script_name:
        - run_twitter_simulation.py
        - run_reddit_simulation.py
        - run_parallel_simulation.py
        - action_logger.py
    """
    try:
        # Script nam trong thu muc backend/scripts/
        scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
        
        # Xac thuc ten script
        allowed_scripts = [
            "run_twitter_simulation.py",
            "run_reddit_simulation.py", 
            "run_parallel_simulation.py",
            "action_logger.py"
        ]
        
        if script_name not in allowed_scripts:
            return jsonify({
                "success": False,
                "error": f"Script khong xac dinh: {script_name}，tuy chon: {allowed_scripts}"
            }), 400
        
        script_path = os.path.join(scripts_dir, script_name)
        
        if not os.path.exists(script_path):
            return jsonify({
                "success": False,
                "error": f"Tep script khong ton tai: {script_name}"
            }), 404
        
        return send_file(
            script_path,
            as_attachment=True,
            download_name=script_name
        )
        
    except Exception as e:
        logger.error(f"Tai script that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API sinh Profile (dung doc lap) ==============

@simulation_bp.route('/generate-profiles', methods=['POST'])
def generate_profiles():
    """
    Sinh OASIS Agent Profile truc tiep tu do thi (khong tao mo phong)
    
    Yeu cau (JSON):
        {
            "graph_id": "mirofish_xxxx",     // bat buoc
            "entity_types": ["Student"],      // tuy chon
            "use_llm": true,                  // tuy chon
            "platform": "reddit"              // tuy chon
        }
    """
    try:
        data = request.get_json() or {}
        
        graph_id = data.get('graph_id')
        if not graph_id:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap graph_id"
            }), 400
        
        entity_types = data.get('entity_types')
        use_llm = data.get('use_llm', True)
        platform = data.get('platform', 'reddit')
        
        reader = ZepEntityReader()
        filtered = reader.filter_defined_entities(
            graph_id=graph_id,
            defined_entity_types=entity_types,
            enrich_with_edges=True
        )
        
        if filtered.filtered_count == 0:
            return jsonify({
                "success": False,
                "error": "Khong tim thay thuc the phu hop"
            }), 400
        
        generator = OasisProfileGenerator()
        profiles = generator.generate_profiles_from_entities(
            entities=filtered.entities,
            use_llm=use_llm
        )
        
        if platform == "reddit":
            profiles_data = [p.to_reddit_format() for p in profiles]
        elif platform == "twitter":
            profiles_data = [p.to_twitter_format() for p in profiles]
        else:
            profiles_data = [p.to_dict() for p in profiles]
        
        return jsonify({
            "success": True,
            "data": {
                "platform": platform,
                "entity_types": list(filtered.entity_types),
                "count": len(profiles_data),
                "profiles": profiles_data
            }
        })
        
    except Exception as e:
        logger.error(f"Sinh Profile that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API dieu khien chay mo phong ==============

@simulation_bp.route('/start', methods=['POST'])
def start_simulation():
    """
    Bat dau chay mo phong

    Yeu cau (JSON):
        {
            "simulation_id": "sim_xxxx",          // bat buoc, ID mo phong
            "platform": "parallel",                // tuy chon: twitter / reddit / parallel (mac dinh)
            "max_rounds": 100,                     // tuy chon: so vong mo phong toi da, dung de gioi han mo phong qua dai
            "enable_graph_memory_update": false,   // tuy chon: co cap nhat dong hoat dong Agent vao bo nho do thi Zep hay khong
            "force": false                         // tuy chon: bat buoc bat dau lai (se dung mo phong dang chay va don dep log)
        }

    Ve tham so force:
        - Khi bat, neu mo phong dang chay hoac da hoan tat, he thong se dung truoc va don dep nhat ky chay
        - Noi dung duoc don dep gom: run_state.json, actions.jsonl, simulation.log, ...
        - Se khong xoa tep cau hinh (simulation_config.json) va tep profile
        - Phu hop cho truong hop can chay lai mo phong

    Ve enable_graph_memory_update:
        - Khi bat, moi hoat dong cua Agent trong mo phong (dang bai, binh luan, thich, ...) se duoc cap nhat vao do thi Zep theo thoi gian thuc
        - Dieu nay giup do thi "ghi nho" qua trinh mo phong de phuc vu phan tich hoac doi thoai AI sau nay
        - Can project lien ket voi mo phong co graph_id hop le
        - Su dung co che cap nhat theo lo de giam so lan goi API

    Tra ve:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "runner_status": "running",
                "process_pid": 12345,
                "twitter_running": true,
                "reddit_running": true,
                "started_at": "2025-12-01T10:00:00",
                "graph_memory_update_enabled": true,  // co bat cap nhat bo nho do thi hay khong
                "force_restarted": true               // co phai khoi dong lai bat buoc hay khong
            }
        }
    """
    try:
        data = request.get_json() or {}

        simulation_id = data.get('simulation_id')
        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap simulation_id"
            }), 400

        platform = data.get('platform', 'parallel')
        max_rounds = data.get('max_rounds')  # tuy chon: so vong mo phong toi da
        enable_graph_memory_update = data.get('enable_graph_memory_update', False)  # tuy chon: co bat cap nhat bo nho do thi hay khong
        force = data.get('force', False)  # tuy chon: bat buoc bat dau lai

        # Xac thuc tham so max_rounds
        if max_rounds is not None:
            try:
                max_rounds = int(max_rounds)
                if max_rounds <= 0:
                    return jsonify({
                        "success": False,
                        "error": "max_rounds phai la so nguyen duong"
                    }), 400
            except (ValueError, TypeError):
                return jsonify({
                    "success": False,
                    "error": "max_rounds phai la mot so nguyen hop le"
                }), 400

        if platform not in ['twitter', 'reddit', 'parallel']:
            return jsonify({
                "success": False,
                "error": f"Loai nen tang khong hop le: {platform}, tuy chon: twitter/reddit/parallel"
            }), 400

        # Kiem tra xem mo phong da san sang chua
        manager = SimulationManager()
        state = manager.get_simulation(simulation_id)

        if not state:
            return jsonify({
                "success": False,
                "error": f"Mo phong khong ton tai: {simulation_id}"
            }), 404

        force_restarted = False
        
        # Xu ly trang thai thong minh: neu viec chuan bi da xong thi cho phep khoi dong lai
        if state.status != SimulationStatus.READY:
            # Kiem tra xem viec chuan bi da hoan tat chua
            is_prepared, prepare_info = _check_simulation_prepared(simulation_id)

            if is_prepared:
                # Viec chuan bi da hoan tat, kiem tra xem co tien trinh dang chay hay khong
                if state.status == SimulationStatus.RUNNING:
                    # Kiem tra xem tien trinh mo phong co thuc su dang chay hay khong
                    run_state = SimulationRunner.get_run_state(simulation_id)
                    if run_state and run_state.runner_status.value == "running":
                        # Tien trinh thuc su dang chay
                        if force:
                            # Che do bat buoc: dung mo phong dang chay
                            logger.info(f"Che do bat buoc: dung mo phong dang chay {simulation_id}")
                            try:
                                SimulationRunner.stop_simulation(simulation_id)
                            except Exception as e:
                                logger.warning(f"Gap canh bao khi dung mo phong: {str(e)}")
                        else:
                            return jsonify({
                                "success": False,
                                "error": f"Mo phong dang chay, vui long goi API /stop de dung truoc hoac dung force=true de bat dau lai bat buoc"
                            }), 400

                # Neu la che do bat buoc, don dep nhat ky chay
                if force:
                    logger.info(f"Che do bat buoc: don dep log mo phong {simulation_id}")
                    cleanup_result = SimulationRunner.cleanup_simulation_logs(simulation_id)
                    if not cleanup_result.get("success"):
                        logger.warning(f"Gap canh bao khi don dep log: {cleanup_result.get('errors')}")
                    force_restarted = True

                # Neu tien trinh khong ton tai hoac da ket thuc, dat lai trang thai ve ready
                logger.info(f"Mo phong {simulation_id} da hoan tat phan chuan bi, dat lai trang thai ve ready (trang thai cu: {state.status.value})")
                state.status = SimulationStatus.READY
                manager._save_simulation_state(state)
            else:
                # Viec chuan bi chua hoan tat
                return jsonify({
                    "success": False,
                    "error": f"Mo phong chua san sang, trang thai hien tai: {state.status.value}, vui long goi API /prepare truoc"
                }), 400
        
        # Lay ID do thi (de cap nhat bo nho do thi)
        graph_id = None
        if enable_graph_memory_update:
            # Lay graph_id tu trang thai mo phong hoac tu project
            graph_id = state.graph_id
            if not graph_id:
                # Thu lay tu project
                project = ProjectManager.get_project(state.project_id)
                if project:
                    graph_id = project.graph_id
            
            if not graph_id:
                return jsonify({
                    "success": False,
                    "error": "De bat cap nhat bo nho do thi can co graph_id hop le; vui long dam bao project da xay dung do thi"
                }), 400
            
            logger.info(f"Bat cap nhat bo nho do thi: simulation_id={simulation_id}, graph_id={graph_id}")
        
        # Khoi dong mo phong
        run_state = SimulationRunner.start_simulation(
            simulation_id=simulation_id,
            platform=platform,
            max_rounds=max_rounds,
            enable_graph_memory_update=enable_graph_memory_update,
            graph_id=graph_id
        )
        
        # Cap nhat trang thai mo phong
        state.status = SimulationStatus.RUNNING
        manager._save_simulation_state(state)
        
        response_data = run_state.to_dict()
        if max_rounds:
            response_data['max_rounds_applied'] = max_rounds
        response_data['graph_memory_update_enabled'] = enable_graph_memory_update
        response_data['force_restarted'] = force_restarted
        if enable_graph_memory_update:
            response_data['graph_id'] = graph_id
        
        return jsonify({
            "success": True,
            "data": response_data
        })
        
    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
        
    except Exception as e:
        logger.error(f"Khoi dong mo phong that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/stop', methods=['POST'])
def stop_simulation():
    """
    Dung mo phong
    
    Yeu cau (JSON):
        {
            "simulation_id": "sim_xxxx"  // bat buoc, ID mo phong
        }
    
    Tra ve:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "runner_status": "stopped",
                "completed_at": "2025-12-01T12:00:00"
            }
        }
    """
    try:
        data = request.get_json() or {}
        
        simulation_id = data.get('simulation_id')
        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap simulation_id"
            }), 400
        
        run_state = SimulationRunner.stop_simulation(simulation_id)
        
        # Cap nhat trang thai mo phong
        manager = SimulationManager()
        state = manager.get_simulation(simulation_id)
        if state:
            state.status = SimulationStatus.PAUSED
            manager._save_simulation_state(state)
        
        return jsonify({
            "success": True,
            "data": run_state.to_dict()
        })
        
    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
        
    except Exception as e:
        logger.error(f"Dung mo phong that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API giam sat trang thai thoi gian thuc ==============

@simulation_bp.route('/<simulation_id>/run-status', methods=['GET'])
def get_run_status(simulation_id: str):
    """
    Lay trang thai chay mo phong theo thoi gian thuc (de frontend polling)
    
    Tra ve:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "runner_status": "running",
                "current_round": 5,
                "total_rounds": 144,
                "progress_percent": 3.5,
                "simulated_hours": 2,
                "total_simulation_hours": 72,
                "twitter_running": true,
                "reddit_running": true,
                "twitter_actions_count": 150,
                "reddit_actions_count": 200,
                "total_actions_count": 350,
                "started_at": "2025-12-01T10:00:00",
                "updated_at": "2025-12-01T10:30:00"
            }
        }
    """
    try:
        run_state = SimulationRunner.get_run_state(simulation_id)
        
        if not run_state:
            return jsonify({
                "success": True,
                "data": {
                    "simulation_id": simulation_id,
                    "runner_status": "idle",
                    "current_round": 0,
                    "total_rounds": 0,
                    "progress_percent": 0,
                    "twitter_actions_count": 0,
                    "reddit_actions_count": 0,
                    "total_actions_count": 0,
                }
            })
        
        return jsonify({
            "success": True,
            "data": run_state.to_dict()
        })
        
    except Exception as e:
        logger.error(f"Lay trang thai chay that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/<simulation_id>/run-status/detail', methods=['GET'])
def get_run_status_detail(simulation_id: str):
    """
    Lay trang thai chay mo phong chi tiet (bao gom tat ca hanh dong)
    
    Dung de frontend hien thi dien bien theo thoi gian thuc
    
    Tham so truy van:
        platform: Loc theo nen tang (twitter/reddit, tuy chon)
    
    Tra ve:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "runner_status": "running",
                "current_round": 5,
                ...
                "all_actions": [
                    {
                        "round_num": 5,
                        "timestamp": "2025-12-01T10:30:00",
                        "platform": "twitter",
                        "agent_id": 3,
                        "agent_name": "Agent Name",
                        "action_type": "CREATE_POST",
                        "action_args": {"content": "..."},
                        "result": null,
                        "success": true
                    },
                    ...
                ],
                "twitter_actions": [...],  # tat ca hanh dong cua nen tang Twitter
                "reddit_actions": [...]    # tat ca hanh dong cua nen tang Reddit
            }
        }
    """
    try:
        run_state = SimulationRunner.get_run_state(simulation_id)
        platform_filter = request.args.get('platform')
        
        if not run_state:
            return jsonify({
                "success": True,
                "data": {
                    "simulation_id": simulation_id,
                    "runner_status": "idle",
                    "all_actions": [],
                    "twitter_actions": [],
                    "reddit_actions": []
                }
            })
        
        # Lay danh sach hanh dong day du
        all_actions = SimulationRunner.get_all_actions(
            simulation_id=simulation_id,
            platform=platform_filter
        )
        
        # Lay hanh dong theo tung nen tang
        twitter_actions = SimulationRunner.get_all_actions(
            simulation_id=simulation_id,
            platform="twitter"
        ) if not platform_filter or platform_filter == "twitter" else []
        
        reddit_actions = SimulationRunner.get_all_actions(
            simulation_id=simulation_id,
            platform="reddit"
        ) if not platform_filter or platform_filter == "reddit" else []
        
        # Lay hanh dong cua vong hien tai (recent_actions chi hien thi vong moi nhat)
        current_round = run_state.current_round
        recent_actions = SimulationRunner.get_all_actions(
            simulation_id=simulation_id,
            platform=platform_filter,
            round_num=current_round
        ) if current_round > 0 else []
        
        # Lay thong tin trang thai co ban
        result = run_state.to_dict()
        result["all_actions"] = [a.to_dict() for a in all_actions]
        result["twitter_actions"] = [a.to_dict() for a in twitter_actions]
        result["reddit_actions"] = [a.to_dict() for a in reddit_actions]
        result["rounds_count"] = len(run_state.rounds)
        # recent_actions chi hien thi noi dung cua vong moi nhat hien tai tren ca hai nen tang
        result["recent_actions"] = [a.to_dict() for a in recent_actions]
        
        return jsonify({
            "success": True,
            "data": result
        })
        
    except Exception as e:
        logger.error(f"Lay trang thai chi tiet that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/<simulation_id>/actions', methods=['GET'])
def get_simulation_actions(simulation_id: str):
    """
    Lay lich su hanh dong Agent trong mo phong
    
    Tham so truy van:
        limit: So luong tra ve (mac dinh la 100)
        offset: Do lech (mac dinh la 0)
        platform: Loc theo nen tang (twitter/reddit)
        agent_id: Loc theo ID Agent
        round_num: Loc theo vong
    
    Tra ve:
        {
            "success": true,
            "data": {
                "count": 100,
                "actions": [...]
            }
        }
    """
    try:
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        platform = request.args.get('platform')
        agent_id = request.args.get('agent_id', type=int)
        round_num = request.args.get('round_num', type=int)
        
        actions = SimulationRunner.get_actions(
            simulation_id=simulation_id,
            limit=limit,
            offset=offset,
            platform=platform,
            agent_id=agent_id,
            round_num=round_num
        )
        
        return jsonify({
            "success": True,
            "data": {
                "count": len(actions),
                "actions": [a.to_dict() for a in actions]
            }
        })
        
    except Exception as e:
        logger.error(f"Lay lich su hanh dong that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/<simulation_id>/timeline', methods=['GET'])
def get_simulation_timeline(simulation_id: str):
    """
    Lay timeline mo phong (tong hop theo vong)
    
    Dung de frontend hien thi thanh tien do va giao dien timeline
    
    Tham so truy van:
        start_round: Vong bat dau (mac dinh la 0)
        end_round: Vong ket thuc (mac dinh la tat ca)
    
    Tra ve thong tin tong hop cua tung vong
    """
    try:
        start_round = request.args.get('start_round', 0, type=int)
        end_round = request.args.get('end_round', type=int)
        
        timeline = SimulationRunner.get_timeline(
            simulation_id=simulation_id,
            start_round=start_round,
            end_round=end_round
        )
        
        return jsonify({
            "success": True,
            "data": {
                "rounds_count": len(timeline),
                "timeline": timeline
            }
        })
        
    except Exception as e:
        logger.error(f"Lay timeline that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/<simulation_id>/agent-stats', methods=['GET'])
def get_agent_stats(simulation_id: str):
    """
    Lay thong ke cua tung Agent
    
    Dung de frontend hien thi xep hang muc do hoat dong cua Agent, phan bo hanh dong, ...
    """
    try:
        stats = SimulationRunner.get_agent_stats(simulation_id)
        
        return jsonify({
            "success": True,
            "data": {
                "agents_count": len(stats),
                "stats": stats
            }
        })
        
    except Exception as e:
        logger.error(f"Lay thong ke Agent that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API truy van co so du lieu ==============

@simulation_bp.route('/<simulation_id>/posts', methods=['GET'])
def get_simulation_posts(simulation_id: str):
    """
    Lay bai dang trong mo phong
    
    Tham so truy van:
        platform: Loai nen tang (twitter/reddit)
        limit: So luong tra ve (mac dinh la 50)
        offset: Do lech
    
    Tra ve danh sach bai dang (doc tu co so du lieu SQLite)
    """
    try:
        platform = request.args.get('platform', 'reddit')
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        sim_dir = os.path.join(
            os.path.dirname(__file__),
            f'../../uploads/simulations/{simulation_id}'
        )
        
        db_file = f"{platform}_simulation.db"
        db_path = os.path.join(sim_dir, db_file)
        
        if not os.path.exists(db_path):
            return jsonify({
                "success": True,
                "data": {
                    "platform": platform,
                    "count": 0,
                    "posts": [],
                    "message": "Co so du lieu khong ton tai, co the mo phong chua chay"
                }
            })
        
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT * FROM post 
                ORDER BY created_at DESC 
                LIMIT ? OFFSET ?
            """, (limit, offset))
            
            posts = [dict(row) for row in cursor.fetchall()]
            
            cursor.execute("SELECT COUNT(*) FROM post")
            total = cursor.fetchone()[0]
            
        except sqlite3.OperationalError:
            posts = []
            total = 0
        
        conn.close()
        
        return jsonify({
            "success": True,
            "data": {
                "platform": platform,
                "total": total,
                "count": len(posts),
                "posts": posts
            }
        })
        
    except Exception as e:
        logger.error(f"Lay bai dang that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/<simulation_id>/comments', methods=['GET'])
def get_simulation_comments(simulation_id: str):
    """
    Lay binh luan trong mo phong (chi Reddit)
    
    Tham so truy van:
        post_id: Loc theo ID bai dang (tuy chon)
        limit: So luong tra ve
        offset: Do lech
    """
    try:
        post_id = request.args.get('post_id')
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        sim_dir = os.path.join(
            os.path.dirname(__file__),
            f'../../uploads/simulations/{simulation_id}'
        )
        
        db_path = os.path.join(sim_dir, "reddit_simulation.db")
        
        if not os.path.exists(db_path):
            return jsonify({
                "success": True,
                "data": {
                    "count": 0,
                    "comments": []
                }
            })
        
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            if post_id:
                cursor.execute("""
                    SELECT * FROM comment 
                    WHERE post_id = ?
                    ORDER BY created_at DESC 
                    LIMIT ? OFFSET ?
                """, (post_id, limit, offset))
            else:
                cursor.execute("""
                    SELECT * FROM comment 
                    ORDER BY created_at DESC 
                    LIMIT ? OFFSET ?
                """, (limit, offset))
            
            comments = [dict(row) for row in cursor.fetchall()]
            
        except sqlite3.OperationalError:
            comments = []
        
        conn.close()
        
        return jsonify({
            "success": True,
            "data": {
                "count": len(comments),
                "comments": comments
            }
        })
        
    except Exception as e:
        logger.error(f"Lay binh luan that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API phong van Interview ==============

@simulation_bp.route('/interview', methods=['POST'])
def interview_agent():
    """
    Phong van mot Agent

    Luu y: tinh nang nay yeu cau moi truong mo phong dang hoat dong (sau khi hoan tat vong lap mo phong se vao che do cho lenh)

    Yeu cau (JSON):
        {
            "simulation_id": "sim_xxxx",       // bat buoc, ID mo phong
            "agent_id": 0,                     // bat buoc, ID Agent
            "prompt": "Ban nghi gi ve van de nay?",  // bat buoc, cau hoi phong van
            "platform": "twitter",             // tuy chon, chi dinh nen tang (twitter/reddit)
                                               // neu khong chi dinh: mo phong hai nen tang se phong van dong thoi tren ca hai nen tang
            "timeout": 60                      // tuy chon, thoi gian cho toi da (giay), mac dinh la 60
        }

    Tra ve (khong chi dinh platform, che do hai nen tang):
        {
            "success": true,
            "data": {
                "agent_id": 0,
                "prompt": "Ban nghi gi ve van de nay?",
                "result": {
                    "agent_id": 0,
                    "prompt": "...",
                    "platforms": {
                        "twitter": {"agent_id": 0, "response": "...", "platform": "twitter"},
                        "reddit": {"agent_id": 0, "response": "...", "platform": "reddit"}
                    }
                },
                "timestamp": "2025-12-08T10:00:01"
            }
        }

    Tra ve (co chi dinh platform):
        {
            "success": true,
            "data": {
                "agent_id": 0,
                "prompt": "Ban nghi gi ve van de nay?",
                "result": {
                    "agent_id": 0,
                    "response": "Toi cho rang...",
                    "platform": "twitter",
                    "timestamp": "2025-12-08T10:00:00"
                },
                "timestamp": "2025-12-08T10:00:01"
            }
        }
    """
    try:
        data = request.get_json() or {}
        
        simulation_id = data.get('simulation_id')
        agent_id = data.get('agent_id')
        prompt = data.get('prompt')
        platform = data.get('platform')  # tuy chon: twitter/reddit/None
        timeout = data.get('timeout', 60)
        
        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap simulation_id"
            }), 400
        
        if agent_id is None:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap agent_id"
            }), 400
        
        if not prompt:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap prompt (cau hoi phong van)"
            }), 400
        
        # Xac thuc tham so platform
        if platform and platform not in ("twitter", "reddit"):
            return jsonify({
                "success": False,
                "error": "Tham so platform chi co the la 'twitter' hoac 'reddit'"
            }), 400
        
        # Kiem tra trang thai moi truong
        if not SimulationRunner.check_env_alive(simulation_id):
            return jsonify({
                "success": False,
                "error": "Moi truong mo phong khong chay hoac da dong. Vui long dam bao mo phong da hoan tat va da vao che do cho lenh."
            }), 400
        
        # Toi uu prompt, them tien to de tranh Agent goi cong cu
        optimized_prompt = optimize_interview_prompt(prompt)
        
        result = SimulationRunner.interview_agent(
            simulation_id=simulation_id,
            agent_id=agent_id,
            prompt=optimized_prompt,
            platform=platform,
            timeout=timeout
        )

        return jsonify({
            "success": result.get("success", False),
            "data": result
        })
        
    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
        
    except TimeoutError as e:
        return jsonify({
            "success": False,
            "error": f"Het thoi gian cho phan hoi Interview: {str(e)}"
        }), 504
        
    except Exception as e:
        logger.error(f"Interview that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/interview/batch', methods=['POST'])
def interview_agents_batch():
    """
    Phong van hang loat nhieu Agent

    Luu y: tinh nang nay yeu cau moi truong mo phong dang hoat dong

    Yeu cau (JSON):
        {
            "simulation_id": "sim_xxxx",       // bat buoc, ID mo phong
            "interviews": [                    // bat buoc, danh sach phong van
                {
                    "agent_id": 0,
                    "prompt": "Ban nghi gi ve A?",
                    "platform": "twitter"      // tuy chon, chi dinh nen tang phong van cho Agent nay
                },
                {
                    "agent_id": 1,
                    "prompt": "Ban nghi gi ve B?"  // neu khong chi dinh platform thi dung gia tri mac dinh
                }
            ],
            "platform": "reddit",              // tuy chon, nen tang mac dinh (se bi platform cua tung muc ghi de)
                                               // neu khong chi dinh: mo phong hai nen tang se phong van moi Agent dong thoi tren ca hai nen tang
            "timeout": 120                     // tuy chon, thoi gian cho toi da (giay), mac dinh la 120
        }

    Tra ve:
        {
            "success": true,
            "data": {
                "interviews_count": 2,
                "result": {
                    "interviews_count": 4,
                    "results": {
                        "twitter_0": {"agent_id": 0, "response": "...", "platform": "twitter"},
                        "reddit_0": {"agent_id": 0, "response": "...", "platform": "reddit"},
                        "twitter_1": {"agent_id": 1, "response": "...", "platform": "twitter"},
                        "reddit_1": {"agent_id": 1, "response": "...", "platform": "reddit"}
                    }
                },
                "timestamp": "2025-12-08T10:00:01"
            }
        }
    """
    try:
        data = request.get_json() or {}

        simulation_id = data.get('simulation_id')
        interviews = data.get('interviews')
        platform = data.get('platform')  # tuy chon: twitter/reddit/None
        timeout = data.get('timeout', 120)

        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap simulation_id"
            }), 400

        if not interviews or not isinstance(interviews, list):
            return jsonify({
                "success": False,
                "error": "Vui long cung cap interviews (danh sach phong van)"
            }), 400

        # Xac thuc tham so platform
        if platform and platform not in ("twitter", "reddit"):
            return jsonify({
                "success": False,
                "error": "Tham so platform chi co the la 'twitter' hoac 'reddit'"
            }), 400

        # Xac thuc tung muc phong van
        for i, interview in enumerate(interviews):
            if 'agent_id' not in interview:
                return jsonify({
                    "success": False,
                    "error": f"Muc phong van thu {i+1} thieu agent_id"
                }), 400
            if 'prompt' not in interview:
                return jsonify({
                    "success": False,
                    "error": f"Muc phong van thu {i+1} thieu prompt"
                }), 400
            # Xac thuc platform cua tung muc (neu co)
            item_platform = interview.get('platform')
            if item_platform and item_platform not in ("twitter", "reddit"):
                return jsonify({
                    "success": False,
                    "error": f"Muc phong van thu {i+1} co platform chi co the la 'twitter' hoac 'reddit'"
                }), 400

        # Kiem tra trang thai moi truong
        if not SimulationRunner.check_env_alive(simulation_id):
            return jsonify({
                "success": False,
                "error": "Moi truong mo phong khong chay hoac da dong. Vui long dam bao mo phong da hoan tat va da vao che do cho lenh."
            }), 400

        # Toi uu prompt cho tung muc phong van, them tien to de tranh Agent goi cong cu
        optimized_interviews = []
        for interview in interviews:
            optimized_interview = interview.copy()
            optimized_interview['prompt'] = optimize_interview_prompt(interview.get('prompt', ''))
            optimized_interviews.append(optimized_interview)

        result = SimulationRunner.interview_agents_batch(
            simulation_id=simulation_id,
            interviews=optimized_interviews,
            platform=platform,
            timeout=timeout
        )

        return jsonify({
            "success": result.get("success", False),
            "data": result
        })

    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except TimeoutError as e:
        return jsonify({
            "success": False,
            "error": f"Het thoi gian cho phan hoi Interview hang loat: {str(e)}"
        }), 504

    except Exception as e:
        logger.error(f"Interview hang loat that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/interview/all', methods=['POST'])
def interview_all_agents():
    """
    Phong van toan bo - dung cung mot cau hoi cho tat ca Agent

    Luu y: tinh nang nay yeu cau moi truong mo phong dang hoat dong

    Yeu cau (JSON):
        {
            "simulation_id": "sim_xxxx",            // bat buoc, ID mo phong
            "prompt": "Ban danh gia tong the van de nay nhu the nao?",  // bat buoc, cau hoi phong van (tat ca Agent dung cung mot cau hoi)
            "platform": "reddit",                   // tuy chon, chi dinh nen tang (twitter/reddit)
                                                    // neu khong chi dinh: mo phong hai nen tang se phong van moi Agent dong thoi tren ca hai nen tang
            "timeout": 180                          // tuy chon, thoi gian cho toi da (giay), mac dinh la 180
        }

    Tra ve:
        {
            "success": true,
            "data": {
                "interviews_count": 50,
                "result": {
                    "interviews_count": 100,
                    "results": {
                        "twitter_0": {"agent_id": 0, "response": "...", "platform": "twitter"},
                        "reddit_0": {"agent_id": 0, "response": "...", "platform": "reddit"},
                        ...
                    }
                },
                "timestamp": "2025-12-08T10:00:01"
            }
        }
    """
    try:
        data = request.get_json() or {}

        simulation_id = data.get('simulation_id')
        prompt = data.get('prompt')
        platform = data.get('platform')  # tuy chon: twitter/reddit/None
        timeout = data.get('timeout', 180)

        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap simulation_id"
            }), 400

        if not prompt:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap prompt (cau hoi phong van)"
            }), 400

        # Xac thuc tham so platform
        if platform and platform not in ("twitter", "reddit"):
            return jsonify({
                "success": False,
                "error": "Tham so platform chi co the la 'twitter' hoac 'reddit'"
            }), 400

        # Kiem tra trang thai moi truong
        if not SimulationRunner.check_env_alive(simulation_id):
            return jsonify({
                "success": False,
                "error": "Moi truong mo phong khong chay hoac da dong. Vui long dam bao mo phong da hoan tat va da vao che do cho lenh."
            }), 400

        # Toi uu prompt, them tien to de tranh Agent goi cong cu
        optimized_prompt = optimize_interview_prompt(prompt)

        result = SimulationRunner.interview_all_agents(
            simulation_id=simulation_id,
            prompt=optimized_prompt,
            platform=platform,
            timeout=timeout
        )

        return jsonify({
            "success": result.get("success", False),
            "data": result
        })

    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except TimeoutError as e:
        return jsonify({
            "success": False,
            "error": f"Het thoi gian cho phan hoi Interview toan bo: {str(e)}"
        }), 504

    except Exception as e:
        logger.error(f"Interview toan bo that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/interview/history', methods=['POST'])
def get_interview_history():
    """
    Lay lich su Interview

    Doc tat ca ban ghi Interview tu co so du lieu mo phong

    Yeu cau (JSON):
        {
            "simulation_id": "sim_xxxx",  // bat buoc, ID mo phong
            "platform": "reddit",          // tuy chon, loai nen tang (reddit/twitter)
                                           // neu khong chi dinh thi tra ve toan bo lich su cua ca hai nen tang
            "agent_id": 0,                 // tuy chon, chi lay lich su phong van cua Agent nay
            "limit": 100                   // tuy chon, so luong tra ve, mac dinh la 100
        }

    Tra ve:
        {
            "success": true,
            "data": {
                "count": 10,
                "history": [
                    {
                        "agent_id": 0,
                        "response": "Toi cho rang...",
                        "prompt": "Ban nghi gi ve van de nay?",
                        "timestamp": "2025-12-08T10:00:00",
                        "platform": "reddit"
                    },
                    ...
                ]
            }
        }
    """
    try:
        data = request.get_json() or {}
        
        simulation_id = data.get('simulation_id')
        platform = data.get('platform')  # neu khong chi dinh thi tra ve lich su cua ca hai nen tang
        agent_id = data.get('agent_id')
        limit = data.get('limit', 100)
        
        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap simulation_id"
            }), 400

        history = SimulationRunner.get_interview_history(
            simulation_id=simulation_id,
            platform=platform,
            agent_id=agent_id,
            limit=limit
        )

        return jsonify({
            "success": True,
            "data": {
                "count": len(history),
                "history": history
            }
        })

    except Exception as e:
        logger.error(f"Lay lich su Interview that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/env-status', methods=['POST'])
def get_env_status():
    """
    Lay trang thai moi truong mo phong

    Kiem tra xem moi truong mo phong con hoat dong hay khong (co the nhan lenh Interview)

    Yeu cau (JSON):
        {
            "simulation_id": "sim_xxxx"  // bat buoc, ID mo phong
        }

    Tra ve:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "env_alive": true,
                "twitter_available": true,
                "reddit_available": true,
                "message": "Moi truong dang chay va co the nhan lenh Interview"
            }
        }
    """
    try:
        data = request.get_json() or {}
        
        simulation_id = data.get('simulation_id')
        
        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap simulation_id"
            }), 400

        env_alive = SimulationRunner.check_env_alive(simulation_id)
        
        # Lay thong tin trang thai chi tiet hon
        env_status = SimulationRunner.get_env_status_detail(simulation_id)

        if env_alive:
            message = "Moi truong dang chay va co the nhan lenh Interview"
        else:
            message = "Moi truong khong chay hoac da dong"

        return jsonify({
            "success": True,
            "data": {
                "simulation_id": simulation_id,
                "env_alive": env_alive,
                "twitter_available": env_status.get("twitter_available", False),
                "reddit_available": env_status.get("reddit_available", False),
                "message": message
            }
        })

    except Exception as e:
        logger.error(f"Lay trang thai moi truong that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@simulation_bp.route('/close-env', methods=['POST'])
def close_simulation_env():
    """
    Dong moi truong mo phong
    
    Gui lenh dong moi truong den mo phong de no thoat khoi che do cho lenh mot cach em ai.
    
    Luu y: thao tac nay khac voi API /stop; /stop se buoc tien trinh ket thuc,
    con API nay se cho phep mo phong dong moi truong va thoat mot cach em ai.
    
    Yeu cau (JSON):
        {
            "simulation_id": "sim_xxxx",  // bat buoc, ID mo phong
            "timeout": 30                  // tuy chon, thoi gian cho toi da (giay), mac dinh la 30
        }
    
    Tra ve:
        {
            "success": true,
            "data": {
                "message": "Lenh dong moi truong da duoc gui",
                "result": {...},
                "timestamp": "2025-12-08T10:00:01"
            }
        }
    """
    try:
        data = request.get_json() or {}
        
        simulation_id = data.get('simulation_id')
        timeout = data.get('timeout', 30)
        
        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Vui long cung cap simulation_id"
            }), 400
        
        result = SimulationRunner.close_simulation_env(
            simulation_id=simulation_id,
            timeout=timeout
        )
        
        # Cap nhat trang thai mo phong
        manager = SimulationManager()
        state = manager.get_simulation(simulation_id)
        if state:
            state.status = SimulationStatus.COMPLETED
            manager._save_simulation_state(state)
        
        return jsonify({
            "success": result.get("success", False),
            "data": result
        })
        
    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
        
    except Exception as e:
        logger.error(f"Dong moi truong that bai: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500
