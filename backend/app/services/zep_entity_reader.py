"""
Dịch vụ đọc và lọc thực thể Zep
Đọc các nút từ đồ thị Zep, lọc ra các nút phù hợp với loại thực thể được định nghĩa trước
"""

import time
from typing import Dict, Any, List, Optional, Set, Callable, TypeVar
from dataclasses import dataclass, field

from zep_cloud.client import Zep

from ..config import Config
from ..utils.logger import get_logger
from ..utils.zep_paging import fetch_all_nodes, fetch_all_edges

logger = get_logger('mirofish.zep_entity_reader')

# Dùng cho kiểu trả về generic
T = TypeVar('T')


@dataclass
class EntityNode:
    """Cấu trúc dữ liệu nút thực thể"""
    uuid: str
    name: str
    labels: List[str]
    summary: str
    attributes: Dict[str, Any]
    # Thông tin các cạnh liên quan
    related_edges: List[Dict[str, Any]] = field(default_factory=list)
    # Thông tin các nút liên quan khác
    related_nodes: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "labels": self.labels,
            "summary": self.summary,
            "attributes": self.attributes,
            "related_edges": self.related_edges,
            "related_nodes": self.related_nodes,
        }
    
    def get_entity_type(self) -> Optional[str]:
        """Lấy loại thực thể (loại trừ nhãn mặc định Entity)"""
        for label in self.labels:
            if label not in ["Entity", "Node"]:
                return label
        return None


@dataclass
class FilteredEntities:
    """Tập thực thể sau khi lọc"""
    entities: List[EntityNode]
    entity_types: Set[str]
    total_count: int
    filtered_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "entities": [e.to_dict() for e in self.entities],
            "entity_types": list(self.entity_types),
            "total_count": self.total_count,
            "filtered_count": self.filtered_count,
        }


class ZepEntityReader:
    """
    Dịch vụ đọc và lọc thực thể Zep
    
    Chức năng chính:
    1. Đọc toàn bộ nút từ đồ thị Zep
    2. Lọc ra các nút khớp loại thực thể được định nghĩa trước (Labels không chỉ có Entity)
    3. Lấy thông tin cạnh liên quan và nút liên kết của từng thực thể
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or Config.ZEP_API_KEY
        if not self.api_key:
            raise ValueError("ZEP_API_KEY chưa được cấu hình")
        
        self.client = Zep(api_key=self.api_key)
    
    def _call_with_retry(
        self, 
        func: Callable[[], T], 
        operation_name: str,
        max_retries: int = 3,
        initial_delay: float = 2.0
    ) -> T:
        """
        Gọi Zep API kèm cơ chế thử lại
        
        Args:
            func: Hàm cần thực thi (lambda hoặc callable không có tham số)
            operation_name: Tên thao tác, dùng cho log
            max_retries: Số lần thử lại tối đa (mặc định 3 lần)
            initial_delay: Độ trễ ban đầu tính bằng giây
            
        Returns:
            Kết quả gọi API
        """
        last_exception = None
        delay = initial_delay
        
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Zep {operation_name} lần thử {attempt + 1} thất bại: {str(e)[:100]}, "
                        f"sẽ thử lại sau {delay:.1f} giây..."
                    )
                    time.sleep(delay)
                    delay *= 2  # Backoff theo cấp số nhân
                else:
                    logger.error(f"Zep {operation_name} vẫn thất bại sau {max_retries} lần thử: {str(e)}")
        
        raise last_exception
    
    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        """
        Lấy toàn bộ nút của đồ thị (có phân trang)

        Args:
            graph_id: ID đồ thị

        Returns:
            Danh sách nút
        """
        logger.info(f"Đang lấy toàn bộ nút của đồ thị {graph_id}...")

        nodes = fetch_all_nodes(self.client, graph_id)

        nodes_data = []
        for node in nodes:
            nodes_data.append({
                "uuid": getattr(node, 'uuid_', None) or getattr(node, 'uuid', ''),
                "name": node.name or "",
                "labels": node.labels or [],
                "summary": node.summary or "",
                "attributes": node.attributes or {},
            })

        logger.info(f"Đã lấy {len(nodes_data)} nút")
        return nodes_data

    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        """
        Lấy toàn bộ cạnh của đồ thị (có phân trang)

        Args:
            graph_id: ID đồ thị

        Returns:
            Danh sách cạnh
        """
        logger.info(f"Đang lấy toàn bộ cạnh của đồ thị {graph_id}...")

        edges = fetch_all_edges(self.client, graph_id)

        edges_data = []
        for edge in edges:
            edges_data.append({
                "uuid": getattr(edge, 'uuid_', None) or getattr(edge, 'uuid', ''),
                "name": edge.name or "",
                "fact": edge.fact or "",
                "source_node_uuid": edge.source_node_uuid,
                "target_node_uuid": edge.target_node_uuid,
                "attributes": edge.attributes or {},
            })

        logger.info(f"Đã lấy {len(edges_data)} cạnh")
        return edges_data
    
    def get_node_edges(self, node_uuid: str) -> List[Dict[str, Any]]:
        """
        Lấy toàn bộ cạnh liên quan của một nút chỉ định (có cơ chế thử lại)
        
        Args:
            node_uuid: UUID của nút
            
        Returns:
            Danh sách cạnh
        """
        try:
            # Dùng cơ chế thử lại để gọi Zep API
            edges = self._call_with_retry(
                func=lambda: self.client.graph.node.get_entity_edges(node_uuid=node_uuid),
                operation_name=f"lấy cạnh của nút (node={node_uuid[:8]}...)"
            )
            
            edges_data = []
            for edge in edges:
                edges_data.append({
                    "uuid": getattr(edge, 'uuid_', None) or getattr(edge, 'uuid', ''),
                    "name": edge.name or "",
                    "fact": edge.fact or "",
                    "source_node_uuid": edge.source_node_uuid,
                    "target_node_uuid": edge.target_node_uuid,
                    "attributes": edge.attributes or {},
                })
            
            return edges_data
        except Exception as e:
            logger.warning(f"Lấy cạnh của nút {node_uuid} thất bại: {str(e)}")
            return []
    
    def filter_defined_entities(
        self, 
        graph_id: str,
        defined_entity_types: Optional[List[str]] = None,
        enrich_with_edges: bool = True
    ) -> FilteredEntities:
        """
        Lọc ra các nút phù hợp với loại thực thể được định nghĩa trước
        
        Logic lọc:
        - Nếu Labels của nút chỉ có "Entity", nghĩa là thực thể này không khớp loại đã định nghĩa trước, bỏ qua
        - Nếu Labels của nút chứa nhãn ngoài "Entity" và "Node", nghĩa là khớp loại đã định nghĩa trước, giữ lại
        
        Args:
            graph_id: ID đồ thị
            defined_entity_types: Danh sách loại thực thể định nghĩa trước (tùy chọn, nếu có thì chỉ giữ các loại này)
            enrich_with_edges: Có lấy thông tin cạnh liên quan của từng thực thể hay không
            
        Returns:
            FilteredEntities: Tập thực thể sau khi lọc
        """
        logger.info(f"Bắt đầu lọc thực thể trong đồ thị {graph_id}...")
        
        # Lấy toàn bộ nút
        all_nodes = self.get_all_nodes(graph_id)
        total_count = len(all_nodes)
        
        # Lấy toàn bộ cạnh để phục vụ tra cứu liên kết tiếp theo
        all_edges = self.get_all_edges(graph_id) if enrich_with_edges else []
        
        # Tạo ánh xạ từ UUID nút sang dữ liệu nút
        node_map = {n["uuid"]: n for n in all_nodes}
        
        # Lọc các thực thể phù hợp
        filtered_entities = []
        entity_types_found = set()
        
        for node in all_nodes:
            labels = node.get("labels", [])
            
            # Logic lọc: Labels phải có nhãn ngoài "Entity" và "Node"
            custom_labels = [l for l in labels if l not in ["Entity", "Node"]]
            
            if not custom_labels:
                # Chỉ có nhãn mặc định, bỏ qua
                continue
            
            # Nếu đã chỉ định loại định nghĩa trước, kiểm tra xem có khớp không
            if defined_entity_types:
                matching_labels = [l for l in custom_labels if l in defined_entity_types]
                if not matching_labels:
                    continue
                entity_type = matching_labels[0]
            else:
                entity_type = custom_labels[0]
            
            entity_types_found.add(entity_type)
            
            # Tạo đối tượng nút thực thể
            entity = EntityNode(
                uuid=node["uuid"],
                name=node["name"],
                labels=labels,
                summary=node["summary"],
                attributes=node["attributes"],
            )
            
            # Lấy cạnh và nút liên quan
            if enrich_with_edges:
                related_edges = []
                related_node_uuids = set()
                
                for edge in all_edges:
                    if edge["source_node_uuid"] == node["uuid"]:
                        related_edges.append({
                            "direction": "outgoing",
                            "edge_name": edge["name"],
                            "fact": edge["fact"],
                            "target_node_uuid": edge["target_node_uuid"],
                        })
                        related_node_uuids.add(edge["target_node_uuid"])
                    elif edge["target_node_uuid"] == node["uuid"]:
                        related_edges.append({
                            "direction": "incoming",
                            "edge_name": edge["name"],
                            "fact": edge["fact"],
                            "source_node_uuid": edge["source_node_uuid"],
                        })
                        related_node_uuids.add(edge["source_node_uuid"])
                
                entity.related_edges = related_edges
                
                # Lấy thông tin cơ bản của các nút liên quan
                related_nodes = []
                for related_uuid in related_node_uuids:
                    if related_uuid in node_map:
                        related_node = node_map[related_uuid]
                        related_nodes.append({
                            "uuid": related_node["uuid"],
                            "name": related_node["name"],
                            "labels": related_node["labels"],
                            "summary": related_node.get("summary", ""),
                        })
                
                entity.related_nodes = related_nodes
            
            filtered_entities.append(entity)
        
        logger.info(f"Lọc xong: tổng số nút {total_count}, phù hợp {len(filtered_entities)}, "
                   f"loại thực thể: {entity_types_found}")
        
        return FilteredEntities(
            entities=filtered_entities,
            entity_types=entity_types_found,
            total_count=total_count,
            filtered_count=len(filtered_entities),
        )
    
    def get_entity_with_context(
        self, 
        graph_id: str, 
        entity_uuid: str
    ) -> Optional[EntityNode]:
        """
        Lấy một thực thể cùng toàn bộ ngữ cảnh của nó (cạnh và nút liên quan, có cơ chế thử lại)
        
        Args:
            graph_id: ID đồ thị
            entity_uuid: UUID của thực thể
            
        Returns:
            EntityNode hoặc None
        """
        try:
            # Dùng cơ chế thử lại để lấy nút
            node = self._call_with_retry(
                func=lambda: self.client.graph.node.get(uuid_=entity_uuid),
                operation_name=f"lấy chi tiết nút (uuid={entity_uuid[:8]}...)"
            )
            
            if not node:
                return None
            
            # Lấy các cạnh của nút
            edges = self.get_node_edges(entity_uuid)
            
            # Lấy toàn bộ nút để phục vụ tra cứu liên kết
            all_nodes = self.get_all_nodes(graph_id)
            node_map = {n["uuid"]: n for n in all_nodes}
            
            # Xử lý cạnh và nút liên quan
            related_edges = []
            related_node_uuids = set()
            
            for edge in edges:
                if edge["source_node_uuid"] == entity_uuid:
                    related_edges.append({
                        "direction": "outgoing",
                        "edge_name": edge["name"],
                        "fact": edge["fact"],
                        "target_node_uuid": edge["target_node_uuid"],
                    })
                    related_node_uuids.add(edge["target_node_uuid"])
                else:
                    related_edges.append({
                        "direction": "incoming",
                        "edge_name": edge["name"],
                        "fact": edge["fact"],
                        "source_node_uuid": edge["source_node_uuid"],
                    })
                    related_node_uuids.add(edge["source_node_uuid"])
            
            # Lấy thông tin nút liên quan
            related_nodes = []
            for related_uuid in related_node_uuids:
                if related_uuid in node_map:
                    related_node = node_map[related_uuid]
                    related_nodes.append({
                        "uuid": related_node["uuid"],
                        "name": related_node["name"],
                        "labels": related_node["labels"],
                        "summary": related_node.get("summary", ""),
                    })
            
            return EntityNode(
                uuid=getattr(node, 'uuid_', None) or getattr(node, 'uuid', ''),
                name=node.name or "",
                labels=node.labels or [],
                summary=node.summary or "",
                attributes=node.attributes or {},
                related_edges=related_edges,
                related_nodes=related_nodes,
            )
            
        except Exception as e:
            logger.error(f"Lấy thực thể {entity_uuid} thất bại: {str(e)}")
            return None
    
    def get_entities_by_type(
        self, 
        graph_id: str, 
        entity_type: str,
        enrich_with_edges: bool = True
    ) -> List[EntityNode]:
        """
        Lấy toàn bộ thực thể theo loại chỉ định
        
        Args:
            graph_id: ID đồ thị
            entity_type: Loại thực thể (ví dụ: "Student", "PublicFigure", ...)
            enrich_with_edges: Có lấy thông tin cạnh liên quan hay không
            
        Returns:
            Danh sách thực thể
        """
        result = self.filter_defined_entities(
            graph_id=graph_id,
            defined_entity_types=[entity_type],
            enrich_with_edges=enrich_with_edges
        )
        return result.entities


