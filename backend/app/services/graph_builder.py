"""
Dich vu xay dung do thi
Giao dien 2: su dung Zep API de tao Standalone Graph
"""

import os
import uuid
import time
import threading
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass

from zep_cloud.client import Zep
from zep_cloud import EpisodeData, EntityEdgeSourceTarget

from ..config import Config
from ..models.task import TaskManager, TaskStatus
from ..utils.zep_paging import call_with_retry, fetch_all_nodes, fetch_all_edges
from .text_processor import TextProcessor


@dataclass
class GraphInfo:
    """Thong tin do thi"""
    graph_id: str
    node_count: int
    edge_count: int
    entity_types: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "entity_types": self.entity_types,
        }


class GraphBuilderService:
    """
    Dich vu xay dung do thi
    Chiu trach nhiem goi Zep API de tao do thi tri thuc
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or Config.ZEP_API_KEY
        if not self.api_key:
            raise ValueError("ZEP_API_KEY chua duoc cau hinh")
        
        self.client = Zep(api_key=self.api_key)
        self.task_manager = TaskManager()
    
    def build_graph_async(
        self,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str = "MiroFish Graph",
        chunk_size: int = 500,
        chunk_overlap: int = 50,
        batch_size: int = 3
    ) -> str:
        """
        Xay dung do thi bat dong bo
        
        Args:
            text: Van ban dau vao
            ontology: Dinh nghia ontology (tu dau ra cua giao dien 1)
            graph_name: Ten do thi
            chunk_size: Kich thuoc moi doan van ban
            chunk_overlap: Do dai phan chong lap giua cac doan
            batch_size: So doan gui trong moi lo
            
        Returns:
            ID tac vu
        """
        # Tao tac vu
        task_id = self.task_manager.create_task(
            task_type="graph_build",
            metadata={
                "graph_name": graph_name,
                "chunk_size": chunk_size,
                "text_length": len(text),
            }
        )
        
        # Thuc hien qua trinh xay dung trong luong nen
        thread = threading.Thread(
            target=self._build_graph_worker,
            args=(task_id, text, ontology, graph_name, chunk_size, chunk_overlap, batch_size)
        )
        thread.daemon = True
        thread.start()
        
        return task_id
    
    def _build_graph_worker(
        self,
        task_id: str,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str,
        chunk_size: int,
        chunk_overlap: int,
        batch_size: int
    ):
        """Luong xu ly xay dung do thi"""
        try:
            self.task_manager.update_task(
                task_id,
                status=TaskStatus.PROCESSING,
                progress=5,
                message="Bat dau xay dung do thi..."
            )
            
            # 1. Tao do thi
            graph_id = self.create_graph(graph_name)
            self.task_manager.update_task(
                task_id,
                progress=10,
                message=f"Da tao do thi: {graph_id}"
            )
            
            # 2. Thiet lap ontology
            self.set_ontology(graph_id, ontology)
            self.task_manager.update_task(
                task_id,
                progress=15,
                message="Da thiet lap ontology"
            )
            
            # 3. Chia van ban thanh cac doan
            chunks = TextProcessor.split_text(text, chunk_size, chunk_overlap)
            total_chunks = len(chunks)
            self.task_manager.update_task(
                task_id,
                progress=20,
                message=f"Van ban da duoc chia thanh {total_chunks} doan"
            )
            
            # 4. Gui du lieu theo tung lo
            episode_uuids = self.add_text_batches(
                graph_id, chunks, batch_size,
                lambda msg, prog: self.task_manager.update_task(
                    task_id,
                    progress=20 + int(prog * 0.4),  # 20-60%
                    message=msg
                )
            )
            
            # 5. Cho Zep xu ly xong
            self.task_manager.update_task(
                task_id,
                progress=60,
                message="Dang cho Zep xu ly du lieu..."
            )
            
            self._wait_for_episodes(
                graph_id,
                episode_uuids,
                lambda msg, prog: self.task_manager.update_task(
                    task_id,
                    progress=60 + int(prog * 0.3),  # 60-90%
                    message=msg
                )
            )
            
            # 6. Lay thong tin do thi
            self.task_manager.update_task(
                task_id,
                progress=90,
                message="Dang lay thong tin do thi..."
            )
            
            graph_info = self._get_graph_info(graph_id)
            
            # Hoan tat
            self.task_manager.complete_task(task_id, {
                "graph_id": graph_id,
                "graph_info": graph_info.to_dict(),
                "chunks_processed": total_chunks,
            })
            
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            self.task_manager.fail_task(task_id, error_msg)
    
    def create_graph(self, name: str) -> str:
        """Tao do thi Zep (phuong thuc cong khai)"""
        graph_id = f"mirofish_{uuid.uuid4().hex[:16]}"
        
        self.client.graph.create(
            graph_id=graph_id,
            name=name,
            description="MiroFish Social Simulation Graph"
        )
        
        return graph_id
    
    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        """Thiet lap ontology cho do thi (phuong thuc cong khai)"""
        import warnings
        from typing import Optional
        from pydantic import Field
        from zep_cloud.external_clients.ontology import EntityModel, EntityText, EdgeModel
        
        # An canh bao cua Pydantic v2 ve Field(default=None)
        # Day la cach dung Zep SDK yeu cau; canh bao den tu viec tao lop dong va co the bo qua an toan
        warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')
        
        # Ten duoc Zep dat truoc, khong the dung lam ten thuoc tinh
        RESERVED_NAMES = {'uuid', 'name', 'group_id', 'name_embedding', 'summary', 'created_at'}
        
        def safe_attr_name(attr_name: str) -> str:
            """Chuyen ten duoc dat truoc thanh ten an toan"""
            if attr_name.lower() in RESERVED_NAMES:
                return f"entity_{attr_name}"
            return attr_name
        
        # Tao kieu thuc the dong
        entity_types = {}
        for entity_def in ontology.get("entity_types", []):
            name = entity_def["name"]
            description = entity_def.get("description", f"A {name} entity.")
            
            # Tao tu dien thuoc tinh va chu giai kieu (Pydantic v2 yeu cau)
            attrs = {"__doc__": description}
            annotations = {}
            
            for attr_def in entity_def.get("attributes", []):
                attr_name = safe_attr_name(attr_def["name"])  # Dung ten an toan
                attr_desc = attr_def.get("description", attr_name)
                # Zep API can description cua Field; day la bat buoc
                attrs[attr_name] = Field(description=attr_desc, default=None)
                annotations[attr_name] = Optional[EntityText]  # Chu giai kieu
            
            attrs["__annotations__"] = annotations
            
            # Tao lop dong
            entity_class = type(name, (EntityModel,), attrs)
            entity_class.__doc__ = description
            entity_types[name] = entity_class
        
        # Tao kieu canh dong
        edge_definitions = {}
        for edge_def in ontology.get("edge_types", []):
            name = edge_def["name"]
            description = edge_def.get("description", f"A {name} relationship.")
            
            # Tao tu dien thuoc tinh va chu giai kieu
            attrs = {"__doc__": description}
            annotations = {}
            
            for attr_def in edge_def.get("attributes", []):
                attr_name = safe_attr_name(attr_def["name"])  # Dung ten an toan
                attr_desc = attr_def.get("description", attr_name)
                # Zep API can description cua Field; day la bat buoc
                attrs[attr_name] = Field(description=attr_desc, default=None)
                annotations[attr_name] = Optional[str]  # Thuoc tinh canh dung kieu str
            
            attrs["__annotations__"] = annotations
            
            # Tao lop dong
            class_name = ''.join(word.capitalize() for word in name.split('_'))
            edge_class = type(class_name, (EdgeModel,), attrs)
            edge_class.__doc__ = description
            
            # Xay dung source_targets
            source_targets = []
            for st in edge_def.get("source_targets", []):
                source_targets.append(
                    EntityEdgeSourceTarget(
                        source=st.get("source", "Entity"),
                        target=st.get("target", "Entity")
                    )
                )
            
            if source_targets:
                edge_definitions[name] = (edge_class, source_targets)
        
        # Goi Zep API de thiet lap ontology
        if entity_types or edge_definitions:
            self.client.graph.set_ontology(
                graph_ids=[graph_id],
                entities=entity_types if entity_types else None,
                edges=edge_definitions if edge_definitions else None,
            )
    
    def add_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int = 3,
        progress_callback: Optional[Callable] = None
    ) -> List[str]:
        """Them van ban vao do thi theo tung lo, tra ve danh sach uuid cua moi episode"""
        episode_uuids = []
        total_chunks = len(chunks)
        
        for i in range(0, total_chunks, batch_size):
            batch_chunks = chunks[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_chunks + batch_size - 1) // batch_size
            
            if progress_callback:
                progress = (i + len(batch_chunks)) / total_chunks
                progress_callback(
                    f"Dang gui lo du lieu {batch_num}/{total_batches} ({len(batch_chunks)} doan)...",
                    progress
                )
            
            # Tao du lieu episode
            episodes = [
                EpisodeData(data=chunk, type="text")
                for chunk in batch_chunks
            ]
            
            # Gui den Zep
            try:
                batch_result = call_with_retry(
                    self.client.graph.add_batch,
                    graph_id=graph_id,
                    episodes=episodes,
                    operation_description=f"add episode batch {batch_num}/{total_batches} (graph={graph_id})",
                )
                
                # Thu thap episode uuid duoc tra ve
                if batch_result and isinstance(batch_result, list):
                    for ep in batch_result:
                        ep_uuid = getattr(ep, 'uuid_', None) or getattr(ep, 'uuid', None)
                        if ep_uuid:
                            episode_uuids.append(ep_uuid)
                
                # Tranh gui yeu cau qua nhanh
                time.sleep(1)
                
            except Exception as e:
                if progress_callback:
                    progress_callback(f"Gui lo {batch_num} that bai: {str(e)}", 0)
                raise
        
        return episode_uuids
    
    def _wait_for_episodes(
        self,
        graph_id: str,
        episode_uuids: List[str],
        progress_callback: Optional[Callable] = None,
        timeout: int = 600,
        poll_interval: float = 12.0
    ):
        """Cho tat ca episode duoc xu ly xong bang cach kiem tra danh sach episode cua graph."""
        if not episode_uuids:
            if progress_callback:
                progress_callback("Khong can cho (khong co episode)", 1.0)
            return
        
        start_time = time.time()
        pending_episodes = set(episode_uuids)
        completed_count = 0
        total_episodes = len(episode_uuids)
        
        if progress_callback:
            progress_callback(f"Bat dau cho xu ly {total_episodes} doan van ban...", 0)
        
        while pending_episodes:
            if time.time() - start_time > timeout:
                if progress_callback:
                    progress_callback(
                        f"Mot so doan van ban da het thoi gian cho, da hoan thanh {completed_count}/{total_episodes}",
                        completed_count / total_episodes
                    )
                break
            
            # Doc theo graph thay vi truy van tung episode de giam nguy co vuot han muc.
            episode_response = call_with_retry(
                self.client.graph.episode.get_by_graph_id,
                graph_id,
                lastn=total_episodes,
                operation_description=f"poll episode processing status (graph={graph_id})",
            )

            processed_episode_uuids = {
                getattr(episode, 'uuid_', None) or getattr(episode, 'uuid', None)
                for episode in (getattr(episode_response, 'episodes', None) or [])
                if getattr(episode, 'processed', False)
            }

            pending_episodes -= {uuid for uuid in processed_episode_uuids if uuid in pending_episodes}
            completed_count = total_episodes - len(pending_episodes)
            
            elapsed = int(time.time() - start_time)
            if progress_callback:
                progress_callback(
                    f"Zep dang xu ly... {completed_count}/{total_episodes} da xong, {len(pending_episodes)} dang cho ({elapsed} giay)",
                    completed_count / total_episodes if total_episodes > 0 else 0
                )
            
            if pending_episodes:
                time.sleep(poll_interval)
        
        if progress_callback:
            progress_callback(f"Xu ly hoan tat: {completed_count}/{total_episodes}", 1.0)
    
    def _get_graph_info(self, graph_id: str) -> GraphInfo:
        """Lay thong tin do thi"""
        # Lay cac nut (phan trang)
        nodes = fetch_all_nodes(self.client, graph_id)

        # Lay cac canh (phan trang)
        edges = fetch_all_edges(self.client, graph_id)

        # Thong ke loai thuc the
        entity_types = set()
        for node in nodes:
            if node.labels:
                for label in node.labels:
                    if label not in ["Entity", "Node"]:
                        entity_types.add(label)

        return GraphInfo(
            graph_id=graph_id,
            node_count=len(nodes),
            edge_count=len(edges),
            entity_types=list(entity_types)
        )
    
    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        """
        Lay du lieu do thi day du (bao gom thong tin chi tiet)
        
        Args:
            graph_id: ID do thi
            
        Returns:
            Tu dien chua nodes va edges, bao gom thong tin thoi gian, thuoc tinh va cac du lieu chi tiet khac
        """
        nodes = fetch_all_nodes(self.client, graph_id)
        edges = fetch_all_edges(self.client, graph_id)

        # Tao anh xa nut de lay ten nut
        node_map = {}
        for node in nodes:
            node_map[node.uuid_] = node.name or ""
        
        nodes_data = []
        for node in nodes:
            # Lay thoi gian tao
            created_at = getattr(node, 'created_at', None)
            if created_at:
                created_at = str(created_at)
            
            nodes_data.append({
                "uuid": node.uuid_,
                "name": node.name,
                "labels": node.labels or [],
                "summary": node.summary or "",
                "attributes": node.attributes or {},
                "created_at": created_at,
            })
        
        edges_data = []
        for edge in edges:
            # Lay thong tin thoi gian
            created_at = getattr(edge, 'created_at', None)
            valid_at = getattr(edge, 'valid_at', None)
            invalid_at = getattr(edge, 'invalid_at', None)
            expired_at = getattr(edge, 'expired_at', None)
            
            # Lay episodes
            episodes = getattr(edge, 'episodes', None) or getattr(edge, 'episode_ids', None)
            if episodes and not isinstance(episodes, list):
                episodes = [str(episodes)]
            elif episodes:
                episodes = [str(e) for e in episodes]
            
            # Lay fact_type
            fact_type = getattr(edge, 'fact_type', None) or edge.name or ""
            
            edges_data.append({
                "uuid": edge.uuid_,
                "name": edge.name or "",
                "fact": edge.fact or "",
                "fact_type": fact_type,
                "source_node_uuid": edge.source_node_uuid,
                "target_node_uuid": edge.target_node_uuid,
                "source_node_name": node_map.get(edge.source_node_uuid, ""),
                "target_node_name": node_map.get(edge.target_node_uuid, ""),
                "attributes": edge.attributes or {},
                "created_at": str(created_at) if created_at else None,
                "valid_at": str(valid_at) if valid_at else None,
                "invalid_at": str(invalid_at) if invalid_at else None,
                "expired_at": str(expired_at) if expired_at else None,
                "episodes": episodes or [],
            })
        
        return {
            "graph_id": graph_id,
            "nodes": nodes_data,
            "edges": edges_data,
            "node_count": len(nodes_data),
            "edge_count": len(edges_data),
        }
    
    def delete_graph(self, graph_id: str):
        """Xoa do thi"""
        self.client.graph.delete(graph_id=graph_id)

