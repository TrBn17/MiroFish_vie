"""Graph API routes using persisted project context on the server."""

import os
import traceback
import threading
from flask import request, jsonify

from . import graph_bp
from ..config import Config
from ..services.ontology_generator import OntologyGenerator
from ..services.graph_builder import GraphBuilderService
from ..services.text_processor import TextProcessor
from ..utils.file_parser import FileParser
from ..utils.logger import get_logger
from ..utils.zep_paging import get_retry_after_seconds, is_rate_limit_error
from ..models.task import TaskManager, TaskStatus
from ..models.project import ProjectManager, ProjectStatus

# Get logger.
logger = get_logger('mirofish.api')


def allowed_file(filename: str) -> bool:
    """Check whether the file extension is allowed."""
    if not filename or '.' not in filename:
        return False
    ext = os.path.splitext(filename)[1].lower().lstrip('.')
    return ext in Config.ALLOWED_EXTENSIONS


# ============== Project management APIs ==============

@graph_bp.route('/project/<project_id>', methods=['GET'])
def get_project(project_id: str):
    """
    Lấy chi tiết dự án.
    """
    project = ProjectManager.get_project(project_id)
    
    if not project:
        return jsonify({
            "success": False,
            "error": f"Project not found: {project_id}"
        }), 404
    
    return jsonify({
        "success": True,
        "data": project.to_dict()
    })


@graph_bp.route('/project/list', methods=['GET'])
def list_projects():
    """
    Liệt kê tất cả dự án.
    """
    limit = request.args.get('limit', 50, type=int)
    projects = ProjectManager.list_projects(limit=limit)
    
    return jsonify({
        "success": True,
        "data": [p.to_dict() for p in projects],
        "count": len(projects)
    })


@graph_bp.route('/project/<project_id>', methods=['DELETE'])
def delete_project(project_id: str):
    """
    Xóa dự án.
    """
    success = ProjectManager.delete_project(project_id)
    
    if not success:
        return jsonify({
            "success": False,
            "error": f"Project not found or deletion failed: {project_id}"
        }), 404
    
    return jsonify({
        "success": True,
        "message": f"Project deleted: {project_id}"
    })


@graph_bp.route('/project/<project_id>/reset', methods=['POST'])
def reset_project(project_id: str):
    """
    Đặt lại trạng thái dự án để xây dựng lại đồ thị.
    """
    project = ProjectManager.get_project(project_id)
    
    if not project:
        return jsonify({
            "success": False,
            "error": f"Project not found: {project_id}"
        }), 404
    
    # Reset back to ontology-generated state when ontology exists.
    if project.ontology:
        project.status = ProjectStatus.ONTOLOGY_GENERATED
    else:
        project.status = ProjectStatus.CREATED
    
    project.graph_id = None
    project.graph_build_task_id = None
    project.error = None
    ProjectManager.save_project(project)
    
    return jsonify({
        "success": True,
        "message": f"Project reset: {project_id}",
        "data": project.to_dict()
    })


# ============== API 1: Tải tệp và sinh ontology ==============

@graph_bp.route('/ontology/generate', methods=['POST'])
def generate_ontology():
    """
    API 1: tải tệp lên, phân tích và sinh định nghĩa ontology.

    Phương thức yêu cầu: `multipart/form-data`

    Tham số:
        files: các tệp tải lên (PDF/MD/TXT), có thể nhiều tệp
        simulation_requirement: mô tả yêu cầu mô phỏng (bắt buộc)
        project_name: tên dự án (tùy chọn)
        additional_context: ghi chú bổ sung (tùy chọn)

    Trả về:
        {
            "success": true,
            "data": {
                "project_id": "proj_xxxx",
                "ontology": {
                    "entity_types": [...],
                    "edge_types": [...],
                    "analysis_summary": "..."
                },
                "files": [...],
                "total_text_length": 12345
            }
        }
    """
    try:
        logger.info("=== Starting ontology generation ===")
        
        # Read request parameters.
        simulation_requirement = request.form.get('simulation_requirement', '')
        project_name = request.form.get('project_name', 'Unnamed Project')
        additional_context = request.form.get('additional_context', '')
        
        logger.debug(f"Project name: {project_name}")
        logger.debug(f"Simulation requirement: {simulation_requirement[:100]}...")
        
        if not simulation_requirement:
            return jsonify({
                "success": False,
                "error": "Please provide simulation_requirement"
            }), 400
        
        # Get uploaded files.
        uploaded_files = request.files.getlist('files')
        if not uploaded_files or all(not f.filename for f in uploaded_files):
            return jsonify({
                "success": False,
                "error": "Please upload at least one document file"
            }), 400
        
        # Create project.
        project = ProjectManager.create_project(name=project_name)
        project.simulation_requirement = simulation_requirement
        logger.info(f"Created project: {project.project_id}")
        
        # Save files and extract text.
        document_texts = []
        all_text = ""
        
        for file in uploaded_files:
            if file and file.filename and allowed_file(file.filename):
                # Save file into the project directory.
                file_info = ProjectManager.save_file_to_project(
                    project.project_id, 
                    file, 
                    file.filename
                )
                project.files.append({
                    "filename": file_info["original_filename"],
                    "size": file_info["size"]
                })
                
                # Extract text.
                text = FileParser.extract_text(file_info["path"])
                text = TextProcessor.preprocess_text(text)
                document_texts.append(text)
                all_text += f"\n\n=== {file_info['original_filename']} ===\n{text}"
        
        if not document_texts:
            ProjectManager.delete_project(project.project_id)
            return jsonify({
                "success": False,
                "error": "No documents were processed successfully. Please check the file formats"
            }), 400
        
        # Save extracted text.
        project.total_text_length = len(all_text)
        ProjectManager.save_extracted_text(project.project_id, all_text)
        logger.info(f"Text extraction complete, total {len(all_text)} characters")
        
        # Generate ontology.
        logger.info("Calling the LLM to generate ontology definitions...")
        generator = OntologyGenerator()
        ontology = generator.generate(
            document_texts=document_texts,
            simulation_requirement=simulation_requirement,
            additional_context=additional_context if additional_context else None
        )
        
        # Save ontology to the project.
        entity_count = len(ontology.get("entity_types", []))
        edge_count = len(ontology.get("edge_types", []))
        logger.info(f"Ontology generation complete: {entity_count} entity types, {edge_count} relationship types")
        
        project.ontology = {
            "entity_types": ontology.get("entity_types", []),
            "edge_types": ontology.get("edge_types", [])
        }
        project.analysis_summary = ontology.get("analysis_summary", "")
        project.status = ProjectStatus.ONTOLOGY_GENERATED
        ProjectManager.save_project(project)
        logger.info(f"=== Ontology generation complete === project_id: {project.project_id}")
        
        return jsonify({
            "success": True,
            "data": {
                "project_id": project.project_id,
                "project_name": project.name,
                "ontology": project.ontology,
                "analysis_summary": project.analysis_summary,
                "files": project.files,
                "total_text_length": project.total_text_length
            }
        })
        
    except Exception as e:
        if is_rate_limit_error(e):
            response = {
                "success": False,
                "error": str(e),
            }
            retry_after_seconds = get_retry_after_seconds(e)
            if retry_after_seconds is not None:
                response["retry_after"] = int(retry_after_seconds)
            return jsonify(response), 429

        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API 2: Xây dựng đồ thị ==============

@graph_bp.route('/build', methods=['POST'])
def build_graph():
    """
    API 2: xây dựng đồ thị theo `project_id`.

    Yêu cầu (JSON):
        {
            "project_id": "proj_xxxx",  // bắt buộc, lấy từ API 1
            "graph_name": "Tên đồ thị", // tùy chọn
            "chunk_size": 500,          // tùy chọn, mặc định 500
            "chunk_overlap": 50         // tùy chọn, mặc định 50
        }

    Trả về:
        {
            "success": true,
            "data": {
                "project_id": "proj_xxxx",
                "task_id": "task_xxxx",
                "message": "Tác vụ xây dựng đồ thị đã được khởi động"
            }
        }
    """
    try:
        logger.info("=== Bắt đầu xây dựng đồ thị ===")
        
        # Kiểm tra cấu hình
        errors = []
        if not Config.ZEP_API_KEY:
            errors.append("ZEP_API_KEY chưa được cấu hình")
        if errors:
            logger.error(f"Lỗi cấu hình: {errors}")
            return jsonify({
                "success": False,
                "error": "Lỗi cấu hình: " + "; ".join(errors)
            }), 500
        
        # Phân tích yêu cầu
        data = request.get_json() or {}
        project_id = data.get('project_id')
        logger.debug(f"Tham số yêu cầu: project_id={project_id}")
        
        if not project_id:
            return jsonify({
                "success": False,
                "error": "Vui lòng cung cấp project_id"
            }), 400
        
        # Lấy dự án
        project = ProjectManager.get_project(project_id)
        if not project:
            return jsonify({
                "success": False,
                "error": f"Không tìm thấy dự án: {project_id}"
            }), 404
        
        # Kiểm tra trạng thái dự án
        force = data.get('force', False)  # Bắt buộc xây dựng lại
        
        if project.status == ProjectStatus.CREATED:
            return jsonify({
                "success": False,
                "error": "Dự án chưa sinh ontology, vui lòng gọi /ontology/generate trước"
            }), 400
        
        if project.status == ProjectStatus.GRAPH_BUILDING and not force:
            return jsonify({
                "success": False,
                "error": "Đồ thị đang được xây dựng, vui lòng không gửi lặp lại. Nếu muốn xây dựng lại bắt buộc, hãy thêm force: true",
                "task_id": project.graph_build_task_id
            }), 400
        
        # Nếu bắt buộc xây dựng lại thì đặt lại trạng thái
        if force and project.status in [ProjectStatus.GRAPH_BUILDING, ProjectStatus.FAILED, ProjectStatus.GRAPH_COMPLETED]:
            project.status = ProjectStatus.ONTOLOGY_GENERATED
            project.graph_id = None
            project.graph_build_task_id = None
            project.error = None
        
        # Lấy cấu hình
        graph_name = data.get('graph_name', project.name or 'MiroFish Graph')
        chunk_size = data.get('chunk_size', project.chunk_size or Config.DEFAULT_CHUNK_SIZE)
        chunk_overlap = data.get('chunk_overlap', project.chunk_overlap or Config.DEFAULT_CHUNK_OVERLAP)
        
        # Cập nhật cấu hình dự án
        project.chunk_size = chunk_size
        project.chunk_overlap = chunk_overlap
        
        # Lấy văn bản đã trích xuất
        text = ProjectManager.get_extracted_text(project_id)
        if not text:
            return jsonify({
                "success": False,
                "error": "Không tìm thấy nội dung văn bản đã trích xuất"
            }), 400
        
        # Lấy ontology
        ontology = project.ontology
        if not ontology:
            return jsonify({
                "success": False,
                "error": "Không tìm thấy định nghĩa ontology"
            }), 400
        
        # Tạo tác vụ bất đồng bộ
        task_manager = TaskManager()
        task_id = task_manager.create_task(f"Xây dựng đồ thị: {graph_name}")
        logger.info(f"Đã tạo tác vụ xây dựng đồ thị: task_id={task_id}, project_id={project_id}")
        
        # Cập nhật trạng thái dự án
        project.status = ProjectStatus.GRAPH_BUILDING
        project.graph_build_task_id = task_id
        ProjectManager.save_project(project)
        
        # Khởi động tác vụ nền
        def build_task():
            build_logger = get_logger('mirofish.build')
            try:
                build_logger.info(f"[{task_id}] Bắt đầu xây dựng đồ thị...")
                task_manager.update_task(
                    task_id, 
                    status=TaskStatus.PROCESSING,
                    message="Đang khởi tạo dịch vụ xây dựng đồ thị..."
                )
                
                # Tạo dịch vụ xây dựng đồ thị
                builder = GraphBuilderService(api_key=Config.ZEP_API_KEY)
                
                # Chia đoạn văn bản
                task_manager.update_task(
                    task_id,
                    message="Đang chia nhỏ văn bản...",
                    progress=5
                )
                chunks = TextProcessor.split_text(
                    text, 
                    chunk_size=chunk_size, 
                    overlap=chunk_overlap
                )
                total_chunks = len(chunks)
                
                # Tạo đồ thị
                task_manager.update_task(
                    task_id,
                    message="Đang tạo đồ thị Zep...",
                    progress=10
                )
                graph_id = builder.create_graph(name=graph_name)
                
                # Cập nhật graph_id của dự án
                project.graph_id = graph_id
                ProjectManager.save_project(project)
                
                # Thiết lập ontology
                task_manager.update_task(
                    task_id,
                    message="Đang thiết lập định nghĩa ontology...",
                    progress=15
                )
                builder.set_ontology(graph_id, ontology)
                
                # Thêm văn bản (`progress_callback` có chữ ký là `(msg, progress_ratio)`)
                def add_progress_callback(msg, progress_ratio):
                    progress = 15 + int(progress_ratio * 40)  # 15% - 55%
                    task_manager.update_task(
                        task_id,
                        message=msg,
                        progress=progress
                    )
                
                task_manager.update_task(
                    task_id,
                    message=f"Bắt đầu thêm {total_chunks} đoạn văn bản...",
                    progress=15
                )
                
                episode_uuids = builder.add_text_batches(
                    graph_id, 
                    chunks,
                    batch_size=3,
                    progress_callback=add_progress_callback
                )
                
                # Chờ Zep xử lý xong bằng cách kiểm tra trạng thái `processed` của từng episode
                task_manager.update_task(
                    task_id,
                    message="Đang chờ Zep xử lý dữ liệu...",
                    progress=55
                )
                
                def wait_progress_callback(msg, progress_ratio):
                    progress = 55 + int(progress_ratio * 35)  # 55% - 90%
                    task_manager.update_task(
                        task_id,
                        message=msg,
                        progress=progress
                    )
                
                builder._wait_for_episodes(graph_id, episode_uuids, wait_progress_callback)
                
                # Lấy dữ liệu đồ thị
                task_manager.update_task(
                    task_id,
                    message="Đang lấy dữ liệu đồ thị...",
                    progress=95
                )
                graph_data = builder.get_graph_data(graph_id)
                
                # Cập nhật trạng thái dự án
                project.status = ProjectStatus.GRAPH_COMPLETED
                ProjectManager.save_project(project)
                
                node_count = graph_data.get("node_count", 0)
                edge_count = graph_data.get("edge_count", 0)
                build_logger.info(f"[{task_id}] Xây dựng đồ thị hoàn tất: graph_id={graph_id}, nút={node_count}, cạnh={edge_count}")
                
                # Hoàn tất
                task_manager.update_task(
                    task_id,
                    status=TaskStatus.COMPLETED,
                    message="Xây dựng đồ thị hoàn tất",
                    progress=100,
                    result={
                        "project_id": project_id,
                        "graph_id": graph_id,
                        "node_count": node_count,
                        "edge_count": edge_count,
                        "chunk_count": total_chunks
                    }
                )
                
            except Exception as e:
                # Cập nhật trạng thái dự án là thất bại
                build_logger.error(f"[{task_id}] Xây dựng đồ thị thất bại: {str(e)}")
                build_logger.debug(traceback.format_exc())
                
                project.status = ProjectStatus.FAILED
                project.error = str(e)
                ProjectManager.save_project(project)
                
                task_manager.update_task(
                    task_id,
                    status=TaskStatus.FAILED,
                    message=f"Xây dựng thất bại: {str(e)}",
                    error=traceback.format_exc()
                )
        
        # Khởi động luồng nền
        thread = threading.Thread(target=build_task, daemon=True)
        thread.start()
        
        return jsonify({
            "success": True,
            "data": {
                "project_id": project_id,
                "task_id": task_id,
                "message": "Tác vụ xây dựng đồ thị đã được khởi động, vui lòng dùng /task/{task_id} để theo dõi tiến độ"
            }
        })
        
    except Exception as e:
        if is_rate_limit_error(e):
            response = {
                "success": False,
                "error": str(e),
            }
            retry_after_seconds = get_retry_after_seconds(e)
            if retry_after_seconds is not None:
                response["retry_after"] = int(retry_after_seconds)
            return jsonify(response), 429

        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API truy vấn tác vụ ==============

@graph_bp.route('/task/<task_id>', methods=['GET'])
def get_task(task_id: str):
    """
    Truy vấn trạng thái tác vụ.
    """
    task = TaskManager().get_task(task_id)
    
    if not task:
        return jsonify({
            "success": False,
            "error": f"Không tìm thấy tác vụ: {task_id}"
        }), 404
    
    return jsonify({
        "success": True,
        "data": task.to_dict()
    })


@graph_bp.route('/tasks', methods=['GET'])
def list_tasks():
    """
    Liệt kê tất cả tác vụ.
    """
    tasks = TaskManager().list_tasks()
    
    return jsonify({
        "success": True,
        "data": tasks,
        "count": len(tasks)
    })


# ============== API dữ liệu đồ thị ==============

@graph_bp.route('/data/<graph_id>', methods=['GET'])
def get_graph_data(graph_id: str):
    """
    Lấy dữ liệu đồ thị, bao gồm nút và cạnh.
    """
    try:
        if not Config.ZEP_API_KEY:
            return jsonify({
                "success": False,
                "error": "ZEP_API_KEY chưa được cấu hình"
            }), 500
        
        builder = GraphBuilderService(api_key=Config.ZEP_API_KEY)
        graph_data = builder.get_graph_data(graph_id)
        
        return jsonify({
            "success": True,
            "data": graph_data
        })
        
    except Exception as e:
        if is_rate_limit_error(e):
            response = {
                "success": False,
                "error": str(e),
            }
            retry_after_seconds = get_retry_after_seconds(e)
            if retry_after_seconds is not None:
                response["retry_after"] = int(retry_after_seconds)
            return jsonify(response), 429

        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@graph_bp.route('/delete/<graph_id>', methods=['DELETE'])
def delete_graph(graph_id: str):
    """
    Xóa đồ thị Zep.
    """
    try:
        if not Config.ZEP_API_KEY:
            return jsonify({
                "success": False,
                "error": "ZEP_API_KEY chưa được cấu hình"
            }), 500
        
        builder = GraphBuilderService(api_key=Config.ZEP_API_KEY)
        builder.delete_graph(graph_id)
        
        return jsonify({
            "success": True,
            "message": f"Đồ thị đã được xóa: {graph_id}"
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500
