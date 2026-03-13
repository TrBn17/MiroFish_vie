"""Report API routes for report generation, retrieval, and chat."""

import os
import traceback
import threading
from flask import request, jsonify, send_file

from . import report_bp
from ..config import Config
from ..services.report_agent import ReportAgent, ReportManager, ReportStatus
from ..services.simulation_manager import SimulationManager
from ..models.project import ProjectManager
from ..models.task import TaskManager, TaskStatus
from ..utils.logger import get_logger

logger = get_logger('mirofish.api.report')


# ============== Report generation APIs ==============

@report_bp.route('/generate', methods=['POST'])
def generate_report():
    """
    Tạo báo cáo phân tích mô phỏng dưới dạng tác vụ bất đồng bộ.

    Đây là thao tác tốn thời gian, API sẽ trả về `task_id` ngay lập tức.
    Dùng `GET /api/report/generate/status` để theo dõi tiến độ.

    Yêu cầu (JSON):
        {
            "simulation_id": "sim_xxxx",    // bắt buộc, ID mô phỏng
            "force_regenerate": false        // tùy chọn, buộc tạo lại
        }

    Trả về:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "task_id": "task_xxxx",
                "status": "generating",
                "message": "Tác vụ tạo báo cáo đã được khởi động"
            }
        }
    """
    try:
        data = request.get_json() or {}
        
        simulation_id = data.get('simulation_id')
        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Please provide simulation_id"
            }), 400
        
        force_regenerate = data.get('force_regenerate', False)
        
        # Load simulation state.
        manager = SimulationManager()
        state = manager.get_simulation(simulation_id)
        
        if not state:
            return jsonify({
                "success": False,
                "error": f"Simulation not found: {simulation_id}"
            }), 404
        
        # Reuse an existing completed report unless regeneration was requested.
        if not force_regenerate:
            existing_report = ReportManager.get_report_by_simulation(simulation_id)
            if existing_report and existing_report.status == ReportStatus.COMPLETED:
                return jsonify({
                    "success": True,
                    "data": {
                        "simulation_id": simulation_id,
                        "report_id": existing_report.report_id,
                        "status": "completed",
                        "message": "Report already exists",
                        "already_generated": True
                    }
                })
        
        # Load project state.
        project = ProjectManager.get_project(state.project_id)
        if not project:
            return jsonify({
                "success": False,
                "error": f"Project not found: {state.project_id}"
            }), 404
        
        graph_id = state.graph_id or project.graph_id
        if not graph_id:
            return jsonify({
                "success": False,
                "error": "Missing graph ID. Please make sure the graph has been built"
            }), 400
        
        simulation_requirement = project.simulation_requirement
        if not simulation_requirement:
            return jsonify({
                "success": False,
                "error": "Missing simulation requirement description"
            }), 400
        
        # Generate report_id up front so it can be returned immediately.
        import uuid
        report_id = f"report_{uuid.uuid4().hex[:12]}"
        
        # Create async task.
        task_manager = TaskManager()
        task_id = task_manager.create_task(
            task_type="report_generate",
            metadata={
                "simulation_id": simulation_id,
                "graph_id": graph_id,
                "report_id": report_id
            }
        )
        
        # Define background task.
        def run_generate():
            try:
                task_manager.update_task(
                    task_id,
                    status=TaskStatus.PROCESSING,
                    progress=0,
                        message="Initializing Report Agent..."
                )
                
                # Create the Report Agent.
                agent = ReportAgent(
                    graph_id=graph_id,
                    simulation_id=simulation_id,
                    simulation_requirement=simulation_requirement
                )
                
                # Progress callback.
                def progress_callback(stage, progress, message):
                    task_manager.update_task(
                        task_id,
                        progress=progress,
                        message=f"[{stage}] {message}"
                    )
                
                # Generate report using the pre-generated report_id.
                report = agent.generate_report(
                    progress_callback=progress_callback,
                    report_id=report_id
                )
                
                # Persist the report.
                ReportManager.save_report(report)
                
                if report.status == ReportStatus.COMPLETED:
                    task_manager.complete_task(
                        task_id,
                        result={
                            "report_id": report.report_id,
                            "simulation_id": simulation_id,
                            "status": "completed"
                        }
                    )
                else:
                    task_manager.fail_task(task_id, report.error or "Report generation failed")
                
            except Exception as e:
                logger.error(f"Report generation failed: {str(e)}")
                task_manager.fail_task(task_id, str(e))
        
        # Start background thread.
        thread = threading.Thread(target=run_generate, daemon=True)
        thread.start()
        
        return jsonify({
            "success": True,
            "data": {
                "simulation_id": simulation_id,
                "report_id": report_id,
                "task_id": task_id,
                "status": "generating",
                "message": "Report generation task started. Query progress via /api/report/generate/status",
                "already_generated": False
            }
        })
        
    except Exception as e:
        logger.error(f"Failed to start report generation task: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@report_bp.route('/generate/status', methods=['POST'])
def get_generate_status():
    """
    Truy vấn tiến độ tạo báo cáo.

    Yêu cầu (JSON):
        {
            "task_id": "task_xxxx",         // tùy chọn, task_id trả về từ generate
            "simulation_id": "sim_xxxx"     // tùy chọn, ID mô phỏng
        }

    Trả về:
        {
            "success": true,
            "data": {
                "task_id": "task_xxxx",
                "status": "processing|completed|failed",
                "progress": 45,
                "message": "..."
            }
        }
    """
    try:
        data = request.get_json() or {}
        
        task_id = data.get('task_id')
        simulation_id = data.get('simulation_id')
        
        # Nếu có `simulation_id` thì kiểm tra trước xem đã có báo cáo hoàn thành hay chưa
        if simulation_id:
            existing_report = ReportManager.get_report_by_simulation(simulation_id)
            if existing_report and existing_report.status == ReportStatus.COMPLETED:
                return jsonify({
                    "success": True,
                    "data": {
                        "simulation_id": simulation_id,
                        "report_id": existing_report.report_id,
                        "status": "completed",
                        "progress": 100,
                        "message": "Report has been generated",
                        "already_completed": True
                    }
                })
        
        if not task_id:
            return jsonify({
                "success": False,
                "error": "Please provide task_id or simulation_id"
            }), 400
        
        task_manager = TaskManager()
        task = task_manager.get_task(task_id)
        
        if not task:
            return jsonify({
                "success": False,
                "error": f"Task not found: {task_id}"
            }), 404
        
        return jsonify({
            "success": True,
            "data": task.to_dict()
        })
        
    except Exception as e:
        logger.error(f"Truy vấn trạng thái tác vụ thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# ============== API lấy báo cáo ==============

@report_bp.route('/<report_id>', methods=['GET'])
def get_report(report_id: str):
    """
    Lấy chi tiết báo cáo.

    Trả về:
        {
            "success": true,
            "data": {
                "report_id": "report_xxxx",
                "simulation_id": "sim_xxxx",
                "status": "completed",
                "outline": {...},
                "markdown_content": "...",
                "created_at": "...",
                "completed_at": "..."
            }
        }
    """
    try:
        report = ReportManager.get_report(report_id)
        
        if not report:
            return jsonify({
                "success": False,
                "error": f"Không tìm thấy báo cáo: {report_id}"
            }), 404
        
        return jsonify({
            "success": True,
            "data": report.to_dict()
        })
        
    except Exception as e:
        logger.error(f"Lấy báo cáo thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@report_bp.route('/by-simulation/<simulation_id>', methods=['GET'])
def get_report_by_simulation(simulation_id: str):
    """
    Lấy báo cáo theo ID mô phỏng.

    Trả về:
        {
            "success": true,
            "data": {
                "report_id": "report_xxxx",
                ...
            }
        }
    """
    try:
        report = ReportManager.get_report_by_simulation(simulation_id)
        
        if not report:
            return jsonify({
                "success": False,
                "error": f"Mô phỏng này hiện chưa có báo cáo: {simulation_id}",
                "has_report": False
            }), 404
        
        return jsonify({
            "success": True,
            "data": report.to_dict(),
            "has_report": True
        })
        
    except Exception as e:
        logger.error(f"Lấy báo cáo thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@report_bp.route('/list', methods=['GET'])
def list_reports():
    """
    Liệt kê tất cả báo cáo.

    Tham số query:
        simulation_id: lọc theo ID mô phỏng (tùy chọn)
        limit: giới hạn số lượng trả về (mặc định 50)

    Trả về:
        {
            "success": true,
            "data": [...],
            "count": 10
        }
    """
    try:
        simulation_id = request.args.get('simulation_id')
        limit = request.args.get('limit', 50, type=int)
        
        reports = ReportManager.list_reports(
            simulation_id=simulation_id,
            limit=limit
        )
        
        return jsonify({
            "success": True,
            "data": [r.to_dict() for r in reports],
            "count": len(reports)
        })
        
    except Exception as e:
        logger.error(f"Liệt kê báo cáo thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@report_bp.route('/<report_id>/download', methods=['GET'])
def download_report(report_id: str):
    """
    Tải báo cáo ở định dạng Markdown.

    Trả về tệp Markdown.
    """
    try:
        report = ReportManager.get_report(report_id)
        
        if not report:
            return jsonify({
                "success": False,
                "error": f"Không tìm thấy báo cáo: {report_id}"
            }), 404
        
        md_path = ReportManager._get_report_markdown_path(report_id)
        
        if not os.path.exists(md_path):
            # Nếu tệp Markdown không tồn tại thì tạo một tệp tạm thời
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
                f.write(report.markdown_content)
                temp_path = f.name
            
            return send_file(
                temp_path,
                as_attachment=True,
                download_name=f"{report_id}.md"
            )
        
        return send_file(
            md_path,
            as_attachment=True,
            download_name=f"{report_id}.md"
        )
        
    except Exception as e:
        logger.error(f"Tải báo cáo thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@report_bp.route('/<report_id>', methods=['DELETE'])
def delete_report(report_id: str):
    """Xóa báo cáo."""
    try:
        success = ReportManager.delete_report(report_id)
        
        if not success:
            return jsonify({
                "success": False,
                "error": f"Không tìm thấy báo cáo: {report_id}"
            }), 404
        
        return jsonify({
            "success": True,
            "message": f"Đã xóa báo cáo: {report_id}"
        })
        
    except Exception as e:
        logger.error(f"Xóa báo cáo thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API hội thoại với Report Agent ==============

@report_bp.route('/chat', methods=['POST'])
def chat_with_report_agent():
    """
    Trò chuyện với Report Agent.

    Report Agent có thể tự gọi các công cụ truy xuất trong lúc hội thoại để trả lời câu hỏi.

    Yêu cầu (JSON):
        {
            "simulation_id": "sim_xxxx",        // bắt buộc, ID mô phỏng
            "message": "Hãy giải thích xu hướng dư luận",    // bắt buộc, tin nhắn người dùng
            "chat_history": [                   // tùy chọn, lịch sử hội thoại
                {"role": "user", "content": "..."},
                {"role": "assistant", "content": "..."}
            ]
        }

    Trả về:
        {
            "success": true,
            "data": {
                "response": "Phản hồi của Agent...",
                "tool_calls": [danh sách công cụ đã gọi],
                "sources": [nguồn thông tin]
            }
        }
    """
    try:
        data = request.get_json() or {}
        
        simulation_id = data.get('simulation_id')
        message = data.get('message')
        chat_history = data.get('chat_history', [])
        
        if not simulation_id:
            return jsonify({
                "success": False,
                "error": "Vui lòng cung cấp simulation_id"
            }), 400
        
        if not message:
            return jsonify({
                "success": False,
                "error": "Vui lòng cung cấp message"
            }), 400
        
        # Lấy thông tin mô phỏng và dự án
        manager = SimulationManager()
        state = manager.get_simulation(simulation_id)
        
        if not state:
            return jsonify({
                "success": False,
                "error": f"Không tìm thấy mô phỏng: {simulation_id}"
            }), 404
        
        project = ProjectManager.get_project(state.project_id)
        if not project:
            return jsonify({
                "success": False,
                "error": f"Không tìm thấy dự án: {state.project_id}"
            }), 404
        
        graph_id = state.graph_id or project.graph_id
        if not graph_id:
            return jsonify({
                "success": False,
                "error": "Thiếu graph_id"
            }), 400
        
        simulation_requirement = project.simulation_requirement or ""
        
        # Tạo Agent và bắt đầu hội thoại
        agent = ReportAgent(
            graph_id=graph_id,
            simulation_id=simulation_id,
            simulation_requirement=simulation_requirement
        )
        
        result = agent.chat(message=message, chat_history=chat_history)
        
        return jsonify({
            "success": True,
            "data": result
        })
        
    except Exception as e:
        logger.error(f"Hội thoại thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API tiến độ và từng phần báo cáo ==============

@report_bp.route('/<report_id>/progress', methods=['GET'])
def get_report_progress(report_id: str):
    """
    Lấy tiến độ tạo báo cáo theo thời gian thực.

    Trả về:
        {
            "success": true,
            "data": {
                "status": "generating",
                "progress": 45,
                "message": "Đang tạo phần: Phát hiện chính",
                "current_section": "Phát hiện chính",
                "completed_sections": ["Tóm tắt điều hành", "Bối cảnh mô phỏng"],
                "updated_at": "2025-12-09T..."
            }
        }
    """
    try:
        progress = ReportManager.get_progress(report_id)
        
        if not progress:
            return jsonify({
                "success": False,
                "error": f"Không tìm thấy báo cáo hoặc không có dữ liệu tiến độ: {report_id}"
            }), 404
        
        return jsonify({
            "success": True,
            "data": progress
        })
        
    except Exception as e:
        logger.error(f"Lấy tiến độ báo cáo thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@report_bp.route('/<report_id>/sections', methods=['GET'])
def get_report_sections(report_id: str):
    """
    Lấy danh sách các phần đã được tạo của báo cáo.

    Frontend có thể poll API này để lấy nội dung từng phần đã hoàn thành mà không cần chờ toàn bộ báo cáo xong.

    Trả về:
        {
            "success": true,
            "data": {
                "report_id": "report_xxxx",
                "sections": [
                    {
                        "filename": "section_01.md",
                        "section_index": 1,
                        "content": "## Tóm tắt điều hành\\n\\n..."
                    },
                    ...
                ],
                "total_sections": 3,
                "is_complete": false
            }
        }
    """
    try:
        sections = ReportManager.get_generated_sections(report_id)
        
        # Lấy trạng thái báo cáo
        report = ReportManager.get_report(report_id)
        is_complete = report is not None and report.status == ReportStatus.COMPLETED
        
        return jsonify({
            "success": True,
            "data": {
                "report_id": report_id,
                "sections": sections,
                "total_sections": len(sections),
                "is_complete": is_complete
            }
        })
        
    except Exception as e:
        logger.error(f"Lấy danh sách phần thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@report_bp.route('/<report_id>/section/<int:section_index>', methods=['GET'])
def get_single_section(report_id: str, section_index: int):
    """
    Lấy nội dung của một phần cụ thể.

    Trả về:
        {
            "success": true,
            "data": {
                "filename": "section_01.md",
                "content": "## Tóm tắt điều hành\\n\\n..."
            }
        }
    """
    try:
        section_path = ReportManager._get_section_path(report_id, section_index)
        
        if not os.path.exists(section_path):
            return jsonify({
                "success": False,
                "error": f"Không tìm thấy phần: section_{section_index:02d}.md"
            }), 404
        
        with open(section_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return jsonify({
            "success": True,
            "data": {
                "filename": f"section_{section_index:02d}.md",
                "section_index": section_index,
                "content": content
            }
        })
        
    except Exception as e:
        logger.error(f"Lấy nội dung phần thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API kiểm tra trạng thái báo cáo ==============

@report_bp.route('/check/<simulation_id>', methods=['GET'])
def check_report_status(simulation_id: str):
    """
    Kiểm tra mô phỏng đã có báo cáo hay chưa và trạng thái của báo cáo.

    Dùng cho frontend để quyết định có mở khóa tính năng Interview hay không.

    Trả về:
        {
            "success": true,
            "data": {
                "simulation_id": "sim_xxxx",
                "has_report": true,
                "report_status": "completed",
                "report_id": "report_xxxx",
                "interview_unlocked": true
            }
        }
    """
    try:
        report = ReportManager.get_report_by_simulation(simulation_id)
        
        has_report = report is not None
        report_status = report.status.value if report else None
        report_id = report.report_id if report else None
        
        # Chỉ mở khóa interview khi báo cáo đã hoàn thành
        interview_unlocked = has_report and report.status == ReportStatus.COMPLETED
        
        return jsonify({
            "success": True,
            "data": {
                "simulation_id": simulation_id,
                "has_report": has_report,
                "report_status": report_status,
                "report_id": report_id,
                "interview_unlocked": interview_unlocked
            }
        })
        
    except Exception as e:
        logger.error(f"Kiểm tra trạng thái báo cáo thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API nhật ký Agent ==============

@report_bp.route('/<report_id>/agent-log', methods=['GET'])
def get_agent_log(report_id: str):
    """
    Lấy nhật ký thực thi chi tiết của Report Agent.

    Cung cấp theo thời gian thực từng bước trong quá trình tạo báo cáo, bao gồm:
    - Bắt đầu báo cáo, bắt đầu/kết thúc lập kế hoạch
    - Bắt đầu từng phần, gọi công cụ, phản hồi từ LLM, hoàn tất
    - Hoàn tất hoặc thất bại của báo cáo

    Tham số query:
        from_line: bắt đầu đọc từ dòng nào (tùy chọn, mặc định 0, dùng cho lấy tăng dần)

    Trả về:
        {
            "success": true,
            "data": {
                "logs": [
                    {
                        "timestamp": "2025-12-13T...",
                        "elapsed_seconds": 12.5,
                        "report_id": "report_xxxx",
                        "action": "tool_call",
                        "stage": "generating",
                        "section_title": "Tóm tắt điều hành",
                        "section_index": 1,
                        "details": {
                            "tool_name": "insight_forge",
                            "parameters": {...},
                            ...
                        }
                    },
                    ...
                ],
                "total_lines": 25,
                "from_line": 0,
                "has_more": false
            }
        }
    """
    try:
        from_line = request.args.get('from_line', 0, type=int)
        
        log_data = ReportManager.get_agent_log(report_id, from_line=from_line)
        
        return jsonify({
            "success": True,
            "data": log_data
        })
        
    except Exception as e:
        logger.error(f"Lấy nhật ký Agent thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@report_bp.route('/<report_id>/agent-log/stream', methods=['GET'])
def stream_agent_log(report_id: str):
    """
    Lấy toàn bộ nhật ký Agent trong một lần.

    Trả về:
        {
            "success": true,
            "data": {
                "logs": [...],
                "count": 25
            }
        }
    """
    try:
        logs = ReportManager.get_agent_log_stream(report_id)
        
        return jsonify({
            "success": True,
            "data": {
                "logs": logs,
                "count": len(logs)
            }
        })
        
    except Exception as e:
        logger.error(f"Lấy nhật ký Agent thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API nhật ký console ==============

@report_bp.route('/<report_id>/console-log', methods=['GET'])
def get_console_log(report_id: str):
    """
    Lấy nhật ký đầu ra console của Report Agent.

    API này trả về log console theo thời gian thực trong quá trình tạo báo cáo (INFO, WARNING, ...).
    Khác với `agent-log`, dữ liệu ở đây không phải JSON có cấu trúc mà là log văn bản thuần theo kiểu console.

    Tham số query:
        from_line: bắt đầu đọc từ dòng nào (tùy chọn, mặc định 0, dùng cho lấy tăng dần)

    Trả về:
        {
            "success": true,
            "data": {
                "logs": [
                    "[19:46:14] INFO: Tìm kiếm hoàn tất: tìm thấy 15 sự kiện liên quan",
                    "[19:46:14] INFO: Tìm kiếm đồ thị: graph_id=xxx, query=...",
                    ...
                ],
                "total_lines": 100,
                "from_line": 0,
                "has_more": false
            }
        }
    """
    try:
        from_line = request.args.get('from_line', 0, type=int)
        
        log_data = ReportManager.get_console_log(report_id, from_line=from_line)
        
        return jsonify({
            "success": True,
            "data": log_data
        })
        
    except Exception as e:
        logger.error(f"Lấy nhật ký console thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@report_bp.route('/<report_id>/console-log/stream', methods=['GET'])
def stream_console_log(report_id: str):
    """
    Lấy toàn bộ nhật ký console trong một lần.

    Trả về:
        {
            "success": true,
            "data": {
                "logs": [...],
                "count": 100
            }
        }
    """
    try:
        logs = ReportManager.get_console_log_stream(report_id)
        
        return jsonify({
            "success": True,
            "data": {
                "logs": logs,
                "count": len(logs)
            }
        })
        
    except Exception as e:
        logger.error(f"Lấy nhật ký console thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# ============== API gọi công cụ (dùng để debug) ==============

@report_bp.route('/tools/search', methods=['POST'])
def search_graph_tool():
    """
    API công cụ tìm kiếm đồ thị, dùng để debug.

    Yêu cầu (JSON):
        {
            "graph_id": "mirofish_xxxx",
            "query": "Từ khóa tìm kiếm",
            "limit": 10
        }
    """
    try:
        data = request.get_json() or {}
        
        graph_id = data.get('graph_id')
        query = data.get('query')
        limit = data.get('limit', 10)
        
        if not graph_id or not query:
            return jsonify({
                "success": False,
                "error": "Vui lòng cung cấp graph_id và query"
            }), 400
        
        from ..services.zep_tools import ZepToolsService
        
        tools = ZepToolsService()
        result = tools.search_graph(
            graph_id=graph_id,
            query=query,
            limit=limit
        )
        
        return jsonify({
            "success": True,
            "data": result.to_dict()
        })
        
    except Exception as e:
        logger.error(f"Tìm kiếm đồ thị thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@report_bp.route('/tools/statistics', methods=['POST'])
def get_graph_statistics_tool():
    """
    API thống kê đồ thị, dùng để debug.

    Yêu cầu (JSON):
        {
            "graph_id": "mirofish_xxxx"
        }
    """
    try:
        data = request.get_json() or {}
        
        graph_id = data.get('graph_id')
        
        if not graph_id:
            return jsonify({
                "success": False,
                "error": "Vui lòng cung cấp graph_id"
            }), 400
        
        from ..services.zep_tools import ZepToolsService
        
        tools = ZepToolsService()
        result = tools.get_graph_statistics(graph_id)
        
        return jsonify({
            "success": True,
            "data": result
        })
        
    except Exception as e:
        logger.error(f"Lấy thống kê đồ thị thất bại: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500
