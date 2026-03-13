"""Zep retrieval tools service.

Wraps graph search, node reads, and edge queries for the Report Agent.

Core tools (optimized):
1. InsightForge (deep analysis retrieval) - strongest hybrid retrieval, auto-generates sub-questions
2. PanoramaSearch (panorama search) - full view including expired content
3. QuickSearch (quick search) - lightweight retrieval
"""

import time
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from zep_cloud.client import Zep

from ..config import Config
from ..utils.logger import get_logger
from ..utils.llm_client import LLMClient
from ..utils.zep_paging import fetch_all_nodes, fetch_all_edges

logger = get_logger('mirofish.zep_tools')


@dataclass
class SearchResult:
    """Search result."""
    facts: List[str]
    edges: List[Dict[str, Any]]
    nodes: List[Dict[str, Any]]
    query: str
    total_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "facts": self.facts,
            "edges": self.edges,
            "nodes": self.nodes,
            "query": self.query,
            "total_count": self.total_count
        }
    
    def to_text(self) -> str:
        """Convert to a text format for LLM consumption."""
        text_parts = [f"Search Query: {self.query}", f"Found {self.total_count} relevant items"]
        
        if self.facts:
            text_parts.append("\n### Relevant Facts:")
            for i, fact in enumerate(self.facts, 1):
                text_parts.append(f"{i}. {fact}")
        
        return "\n".join(text_parts)


@dataclass
class NodeInfo:
    """Node info."""
    uuid: str
    name: str
    labels: List[str]
    summary: str
    attributes: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "labels": self.labels,
            "summary": self.summary,
            "attributes": self.attributes
        }
    
    def to_text(self) -> str:
        """Convert to a text format."""
        entity_type = next((l for l in self.labels if l not in ["Entity", "Node"]), "Unknown")
        return f"Entity: {self.name} (Type: {entity_type})\nSummary: {self.summary}"


@dataclass
class EdgeInfo:
    """Edge info."""
    uuid: str
    name: str
    fact: str
    source_node_uuid: str
    target_node_uuid: str
    source_node_name: Optional[str] = None
    target_node_name: Optional[str] = None
    # Temporal fields
    created_at: Optional[str] = None
    valid_at: Optional[str] = None
    invalid_at: Optional[str] = None
    expired_at: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "fact": self.fact,
            "source_node_uuid": self.source_node_uuid,
            "target_node_uuid": self.target_node_uuid,
            "source_node_name": self.source_node_name,
            "target_node_name": self.target_node_name,
            "created_at": self.created_at,
            "valid_at": self.valid_at,
            "invalid_at": self.invalid_at,
            "expired_at": self.expired_at
        }
    
    def to_text(self, include_temporal: bool = False) -> str:
        """Convert to a text format."""
        source = self.source_node_name or self.source_node_uuid[:8]
        target = self.target_node_name or self.target_node_uuid[:8]
        base_text = f"Relation: {source} --[{self.name}]--> {target}\nFact: {self.fact}"
        
        if include_temporal:
            valid_at = self.valid_at or "Unknown"
            invalid_at = self.invalid_at or "Present"
            base_text += f"\nValidity: {valid_at} - {invalid_at}"
            if self.expired_at:
                base_text += f" (Expired at: {self.expired_at})"
        
        return base_text
    
    @property
    def is_expired(self) -> bool:
        """Whether the edge has expired."""
        return self.expired_at is not None
    
    @property
    def is_invalid(self) -> bool:
        """Whether the edge is invalid."""
        return self.invalid_at is not None


@dataclass
class InsightForgeResult:
    """
    Deep analysis retrieval result (InsightForge).

    Contains retrieval results for multiple sub-questions plus a synthesized view.
    """
    query: str
    simulation_requirement: str
    sub_queries: List[str]
    
    # Retrieval results by dimension
    semantic_facts: List[str] = field(default_factory=list)  # Semantic search results
    entity_insights: List[Dict[str, Any]] = field(default_factory=list)  # Entity insights
    relationship_chains: List[str] = field(default_factory=list)  # Relationship chains
    
    # Statistics
    total_facts: int = 0
    total_entities: int = 0
    total_relationships: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "simulation_requirement": self.simulation_requirement,
            "sub_queries": self.sub_queries,
            "semantic_facts": self.semantic_facts,
            "entity_insights": self.entity_insights,
            "relationship_chains": self.relationship_chains,
            "total_facts": self.total_facts,
            "total_entities": self.total_entities,
            "total_relationships": self.total_relationships
        }
    
    def to_text(self) -> str:
        """Convert to a detailed text format for LLM consumption."""
        text_parts = [
            "## Deep Analysis for Future Forecast",
            f"Analysis Question: {self.query}",
            f"Forecast Scenario: {self.simulation_requirement}",
            "\n### Forecast Data Summary",
            f"- Relevant forecast facts: {self.total_facts}",
            f"- Entities involved: {self.total_entities}",
            f"- Relationship chains: {self.total_relationships}",
        ]
        
        # Sub-questions
        if self.sub_queries:
            text_parts.append("\n### Sub-questions")
            for i, sq in enumerate(self.sub_queries, 1):
                text_parts.append(f"{i}. {sq}")
        
        # Semantic search results
        if self.semantic_facts:
            text_parts.append("\n### [Key Facts] (Quote these verbatim in the report)")
            for i, fact in enumerate(self.semantic_facts, 1):
                text_parts.append(f"{i}. \"{fact}\"")
        
        # Entity insights
        if self.entity_insights:
            text_parts.append("\n### [Core Entities]")
            for entity in self.entity_insights:
                text_parts.append(f"- **{entity.get('name', 'Unknown')}** ({entity.get('type', 'Entity')})")
                if entity.get('summary'):
                    text_parts.append(f"  Summary: \"{entity.get('summary')}\"")
                if entity.get('related_facts'):
                    text_parts.append(f"  Related facts: {len(entity.get('related_facts', []))}")
        
        # Relationship chains
        if self.relationship_chains:
            text_parts.append("\n### [Relationship Chains]")
            for chain in self.relationship_chains:
                text_parts.append(f"- {chain}")
        
        return "\n".join(text_parts)


@dataclass
class PanoramaResult:
    """
    Panorama search result (Panorama).

    Includes all relevant info, including expired content.
    """
    query: str
    
    # All nodes
    all_nodes: List[NodeInfo] = field(default_factory=list)
    # All edges, including expired ones
    all_edges: List[EdgeInfo] = field(default_factory=list)
    # Currently valid facts
    active_facts: List[str] = field(default_factory=list)
    # Expired or invalid facts (historical record)
    historical_facts: List[str] = field(default_factory=list)
    
    # Statistics
    total_nodes: int = 0
    total_edges: int = 0
    active_count: int = 0
    historical_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "all_nodes": [n.to_dict() for n in self.all_nodes],
            "all_edges": [e.to_dict() for e in self.all_edges],
            "active_facts": self.active_facts,
            "historical_facts": self.historical_facts,
            "total_nodes": self.total_nodes,
            "total_edges": self.total_edges,
            "active_count": self.active_count,
            "historical_count": self.historical_count
        }
    
    def to_text(self) -> str:
        """Convert to a text format (full version, not truncated)."""
        text_parts = [
            "## Panorama Search Results (Future Panorama View)",
            f"Query: {self.query}",
            "\n### Summary",
            f"- Total nodes: {self.total_nodes}",
            f"- Total edges: {self.total_edges}",
            f"- Active facts: {self.active_count}",
            f"- Historical/expired facts: {self.historical_count}",
        ]
        
        # Currently valid facts (full output, no truncation)
        if self.active_facts:
            text_parts.append("\n### [Active Facts] (Latest simulation output)")
            for i, fact in enumerate(self.active_facts, 1):
                text_parts.append(f"{i}. \"{fact}\"")
        
        # Historical or expired facts (full output, no truncation)
        if self.historical_facts:
            text_parts.append("\n### [Historical/Expired Facts] (Evolution record)")
            for i, fact in enumerate(self.historical_facts, 1):
                text_parts.append(f"{i}. \"{fact}\"")
        
        # Key entities (full output, no truncation)
        if self.all_nodes:
            text_parts.append("\n### [Involved Entities]")
            for node in self.all_nodes:
                entity_type = next((l for l in node.labels if l not in ["Entity", "Node"]), "Entity")
                text_parts.append(f"- **{node.name}** ({entity_type})")
        
        return "\n".join(text_parts)


@dataclass
class AgentInterview:
    """Single agent interview result."""
    agent_name: str
    agent_role: str  # Role type, such as student, teacher, or media
    agent_bio: str  # Bio
    question: str  # Interview question
    response: str  # Interview answer
    key_quotes: List[str] = field(default_factory=list)  # Key quotes
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_name": self.agent_name,
            "agent_role": self.agent_role,
            "agent_bio": self.agent_bio,
            "question": self.question,
            "response": self.response,
            "key_quotes": self.key_quotes
        }
    
    def to_text(self) -> str:
        text = f"**{self.agent_name}** ({self.agent_role})\n"
        # Show the full agent bio without truncation.
        text += f"Bio: {self.agent_bio}\n\n"
        text += f"**Q:** {self.question}\n\n"
        text += f"**A:** {self.response}\n"
        if self.key_quotes:
            text += "\nKey Quotes:\n"
            for quote in self.key_quotes:
                # Normalize quote characters.
                clean_quote = quote.replace('\u201c', '').replace('\u201d', '').replace('"', '')
                clean_quote = clean_quote.replace('\u300c', '').replace('\u300d', '')
                clean_quote = clean_quote.strip()
                # Strip leading punctuation.
                leading_punct = ',;:!?\n\r\t ' + '\uFF0C\uFF1B\uFF1A\u3001\u3002\uFF01\uFF1F'
                while clean_quote and clean_quote[0] in leading_punct:
                    clean_quote = clean_quote[1:]
                # Filter out garbage that contains question numbers (Question 1-9)
                skip = False
                for d in '123456789':
                    if f'\u95ee\u9898{d}' in clean_quote:
                        skip = True
                        break
                if skip:
                    continue
                # Trim overlong content at sentence boundaries when possible.
                if len(clean_quote) > 150:
                    dot_pos = clean_quote.find('\u3002', 80)
                    if dot_pos > 0:
                        clean_quote = clean_quote[:dot_pos + 1]
                    else:
                        clean_quote = clean_quote[:147] + "..."
                if clean_quote and len(clean_quote) >= 10:
                    text += f'> "{clean_quote}"\n'
        return text


@dataclass
class InterviewResult:
    """
    Interview result.

    Contains interview answers from multiple simulated agents.
    """
    interview_topic: str  # Interview topic
    interview_questions: List[str]  # Interview question list
    
    # Selected agents
    selected_agents: List[Dict[str, Any]] = field(default_factory=list)
    # Interview responses per agent
    interviews: List[AgentInterview] = field(default_factory=list)
    
    # Rationale for selecting agents
    selection_reasoning: str = ""
    # Aggregated interview summary
    summary: str = ""
    
    # Statistics
    total_agents: int = 0
    interviewed_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "interview_topic": self.interview_topic,
            "interview_questions": self.interview_questions,
            "selected_agents": self.selected_agents,
            "interviews": [i.to_dict() for i in self.interviews],
            "selection_reasoning": self.selection_reasoning,
            "summary": self.summary,
            "total_agents": self.total_agents,
            "interviewed_count": self.interviewed_count
        }
    
    def to_text(self) -> str:
        """Convert to a detailed text format for LLM consumption and report quoting."""
        text_parts = [
            "## Deep Interview Report",
            f"Interview Topic: {self.interview_topic}",
            f"Interview Count: {self.interviewed_count} / {self.total_agents} simulated agents",
            "\n### Interviewee Selection Rationale",
            self.selection_reasoning or "(auto-selected)",
            "\n---",
            "\n### Interview Transcript",
        ]

        if self.interviews:
            for i, interview in enumerate(self.interviews, 1):
                text_parts.append(f"\n#### Interview #{i}: {interview.agent_name}")
                text_parts.append(interview.to_text())
                text_parts.append("\n---")
        else:
            text_parts.append("(no interviews)\n\n---")

        text_parts.append("\n### Interview Summary and Key Insights")
        text_parts.append(self.summary or "(no summary)")

        return "\n".join(text_parts)


class ZepToolsService:
    """
    Zep retrieval tools service.

    Core tools (optimized):
    1. insight_forge - deep analysis retrieval (auto sub-questions + multi-dimensional retrieval)
    2. panorama_search - panorama search (includes expired content)
    3. quick_search - quick search
    4. interview_agents - deep interviews (multi-perspective opinions from simulated agents)

    Basic tools:
    - search_graph - graph semantic search
    - get_all_nodes - fetch all nodes
    - get_all_edges - fetch all edges (with temporal info)
    - get_node_detail - fetch node details
    - get_node_edges - fetch edges related to a node
    - get_entities_by_type - fetch entities by type
    - get_entity_summary - fetch entity relationship summary
    """
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 2.0
    
    def __init__(self, api_key: Optional[str] = None, llm_client: Optional[LLMClient] = None):
        self.api_key = api_key or Config.ZEP_API_KEY
        if not self.api_key:
            raise ValueError("ZEP_API_KEY is not configured")
        
        self.client = Zep(api_key=self.api_key)
        # LLM client used by InsightForge to generate sub-questions
        self._llm_client = llm_client
        logger.info("ZepToolsService initialized")
    
    @property
    def llm(self) -> LLMClient:
        """Lazily initialize the LLM client."""
        if self._llm_client is None:
            self._llm_client = LLMClient()
        return self._llm_client
    
    def _call_with_retry(self, func, operation_name: str, max_retries: int = None):
        """API call with retry."""
        max_retries = max_retries or self.MAX_RETRIES
        last_exception = None
        delay = self.RETRY_DELAY
        
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Zep {operation_name} attempt {attempt + 1} failed: {str(e)[:100]}, "
                        f"retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.error(f"Zep {operation_name} failed after {max_retries} attempts: {str(e)}")
        
        raise last_exception
    
    def search_graph(
        self, 
        graph_id: str, 
        query: str, 
        limit: int = 10,
        scope: str = "edges"
    ) -> SearchResult:
        """Graph semantic search.

        Uses hybrid search (semantic + BM25) to find relevant info in a graph.
        Falls back to local keyword matching when Zep Cloud search is unavailable.

        Args:
            graph_id: Graph ID (Standalone Graph)
            query: Search query
            limit: Max number of results
            scope: Search scope: "edges" or "nodes"

        Returns:
            SearchResult: Search results
        """
        logger.info(f"Graph search: graph_id={graph_id}, query={query[:50]}...")
        
        
        # Try Zep Cloud Search API
        try:
            search_results = self._call_with_retry(
                func=lambda: self.client.graph.search(
                    graph_id=graph_id,
                    query=query,
                    limit=limit,
                    scope=scope,
                    reranker="cross_encoder"
                ),
                operation_name=f"Graph search (graph={graph_id})"
            )
            
            facts = []
            edges = []
            nodes = []
            
            # Parse edge search results
            if hasattr(search_results, 'edges') and search_results.edges:
                for edge in search_results.edges:
                    if hasattr(edge, 'fact') and edge.fact:
                        facts.append(edge.fact)
                    edges.append({
                        "uuid": getattr(edge, 'uuid_', None) or getattr(edge, 'uuid', ''),
                        "name": getattr(edge, 'name', ''),
                        "fact": getattr(edge, 'fact', ''),
                        "source_node_uuid": getattr(edge, 'source_node_uuid', ''),
                        "target_node_uuid": getattr(edge, 'target_node_uuid', ''),
                    })
            
            # Parse node search results
            if hasattr(search_results, 'nodes') and search_results.nodes:
                for node in search_results.nodes:
                    nodes.append({
                        "uuid": getattr(node, 'uuid_', None) or getattr(node, 'uuid', ''),
                        "name": getattr(node, 'name', ''),
                        "labels": getattr(node, 'labels', []),
                        "summary": getattr(node, 'summary', ''),
                    })
                        # Node summaries also count as facts
                    if hasattr(node, 'summary') and node.summary:
                        facts.append(f"[{node.name}]: {node.summary}")
            
                    logger.info(f"Search complete: found {len(facts)} relevant facts")
            
            return SearchResult(
                facts=facts,
                edges=edges,
                nodes=nodes,
                query=query,
                total_count=len(facts)
            )
            
        except Exception as e:
            logger.warning(f"Zep Search API failed; falling back to local search: {str(e)}")
            # Fallback: local keyword matching
            return self._local_search(graph_id, query, limit, scope)
    
    def _local_search(
        self, 
        graph_id: str, 
        query: str, 
        limit: int = 10,
        scope: str = "edges"
    ) -> SearchResult:
        """Local keyword matching search (fallback for Zep Cloud search).

        Fetches all edges/nodes and scores them by simple keyword matching.

        Args:
            graph_id: Graph ID
            query: Search query
            limit: Max number of results
            scope: Search scope

        Returns:
            SearchResult: Search results
        """
        logger.info(f"Using local search: query={query[:30]}...")
        
        facts = []
        edges_result = []
        nodes_result = []
        
        # Extract keywords (simple tokenization)
        query_lower = query.lower()
        keywords = [w.strip() for w in query_lower.replace(',', ' ').replace('，', ' ').split() if len(w.strip()) > 1]
        
        def match_score(text: str) -> int:
            """Compute a simple match score between the text and the query."""
            if not text:
                return 0
            text_lower = text.lower()
            # Full query match
            if query_lower in text_lower:
                return 100
            # Keyword match
            score = 0
            for keyword in keywords:
                if keyword in text_lower:
                    score += 10
            return score
        
        try:
            if scope in ["edges", "both"]:
                # Fetch and score all edges
                all_edges = self.get_all_edges(graph_id)
                scored_edges = []
                for edge in all_edges:
                    score = match_score(edge.fact) + match_score(edge.name)
                    if score > 0:
                        scored_edges.append((score, edge))
                
                # Sort by score
                scored_edges.sort(key=lambda x: x[0], reverse=True)
                
                for score, edge in scored_edges[:limit]:
                    if edge.fact:
                        facts.append(edge.fact)
                    edges_result.append({
                        "uuid": edge.uuid,
                        "name": edge.name,
                        "fact": edge.fact,
                        "source_node_uuid": edge.source_node_uuid,
                        "target_node_uuid": edge.target_node_uuid,
                    })
            
            if scope in ["nodes", "both"]:
                # Fetch and score all nodes
                all_nodes = self.get_all_nodes(graph_id)
                scored_nodes = []
                for node in all_nodes:
                    score = match_score(node.name) + match_score(node.summary)
                    if score > 0:
                        scored_nodes.append((score, node))
                
                scored_nodes.sort(key=lambda x: x[0], reverse=True)
                
                for score, node in scored_nodes[:limit]:
                    nodes_result.append({
                        "uuid": node.uuid,
                        "name": node.name,
                        "labels": node.labels,
                        "summary": node.summary,
                    })
                    if node.summary:
                        facts.append(f"[{node.name}]: {node.summary}")
            
            logger.info(f"Local search complete: found {len(facts)} relevant facts")
            
        except Exception as e:
            logger.error(f"Local search failed: {str(e)}")
        
        return SearchResult(
            facts=facts,
            edges=edges_result,
            nodes=nodes_result,
            query=query,
            total_count=len(facts)
        )
    
    def get_all_nodes(self, graph_id: str) -> List[NodeInfo]:
        """
        Lay toan bo cac node trong do thi theo cach phan trang.

        Args:
            graph_id: ID cua do thi.

        Returns:
            Danh sach node.
        """
        logger.info(f"Dang lay toan bo node cua do thi {graph_id}...")

        nodes = fetch_all_nodes(self.client, graph_id)

        result = []
        for node in nodes:
            node_uuid = getattr(node, 'uuid_', None) or getattr(node, 'uuid', None) or ""
            result.append(NodeInfo(
                uuid=str(node_uuid) if node_uuid else "",
                name=node.name or "",
                labels=node.labels or [],
                summary=node.summary or "",
                attributes=node.attributes or {}
            ))

        logger.info(f"Da lay duoc {len(result)} node")
        return result

    def get_all_edges(self, graph_id: str, include_temporal: bool = True) -> List[EdgeInfo]:
        """Fetch all edges in a graph (paged, optionally including temporal fields)."""
        logger.info(f"Fetching all edges for graph {graph_id}...")

        edges = fetch_all_edges(self.client, graph_id)

        result = []
        for edge in edges:
            edge_uuid = getattr(edge, 'uuid_', None) or getattr(edge, 'uuid', None) or ""
            edge_info = EdgeInfo(
                uuid=str(edge_uuid) if edge_uuid else "",
                name=edge.name or "",
                fact=edge.fact or "",
                source_node_uuid=edge.source_node_uuid or "",
                target_node_uuid=edge.target_node_uuid or ""
            )

            # Add temporal fields
            if include_temporal:
                edge_info.created_at = getattr(edge, 'created_at', None)
                edge_info.valid_at = getattr(edge, 'valid_at', None)
                edge_info.invalid_at = getattr(edge, 'invalid_at', None)
                edge_info.expired_at = getattr(edge, 'expired_at', None)

            result.append(edge_info)

        logger.info(f"Fetched {len(result)} edges")
        return result
    
    def get_node_detail(self, node_uuid: str) -> Optional[NodeInfo]:
        """Fetch detailed info for a single node."""
        logger.info(f"Fetching node detail: {node_uuid[:8]}...")
        
        try:
            node = self._call_with_retry(
                func=lambda: self.client.graph.node.get(uuid_=node_uuid),
                operation_name=f"Fetch node detail (uuid={node_uuid[:8]}...)"
            )
            
            if not node:
                return None
            
            return NodeInfo(
                uuid=getattr(node, 'uuid_', None) or getattr(node, 'uuid', ''),
                name=node.name or "",
                labels=node.labels or [],
                summary=node.summary or "",
                attributes=node.attributes or {}
            )
        except Exception as e:
            logger.error(f"Failed to fetch node detail: {str(e)}")
            return None
    
    def get_node_edges(self, graph_id: str, node_uuid: str) -> List[EdgeInfo]:
        """Fetch all edges related to the given node."""
        logger.info(f"Fetching edges related to node {node_uuid[:8]}...")
        
        try:
            # Fetch all edges, then filter
            all_edges = self.get_all_edges(graph_id)
            
            result = []
            for edge in all_edges:
                # Check whether this edge is related (as source or target)
                if edge.source_node_uuid == node_uuid or edge.target_node_uuid == node_uuid:
                    result.append(edge)
            
            logger.info(f"Found {len(result)} related edges")
            return result
            
        except Exception as e:
            logger.warning(f"Failed to fetch node edges: {str(e)}")
            return []
    
    def get_entities_by_type(
        self, 
        graph_id: str, 
        entity_type: str
    ) -> List[NodeInfo]:
        """Fetch entities by type/label."""
        logger.info(f"Fetching entities of type {entity_type}...")
        
        all_nodes = self.get_all_nodes(graph_id)
        
        filtered = []
        for node in all_nodes:
            # Check whether labels contain the type
            if entity_type in node.labels:
                filtered.append(node)
        
        logger.info(f"Found {len(filtered)} entities of type {entity_type}")
        return filtered
    
    def get_entity_summary(
        self, 
        graph_id: str, 
        entity_name: str
    ) -> Dict[str, Any]:
        """Get a relationship summary for the given entity."""
        logger.info(f"Getting relationship summary for entity {entity_name}...")
        
        # Search for facts related to the entity
        search_result = self.search_graph(
            graph_id=graph_id,
            query=entity_name,
            limit=20
        )
        
        # Try to find the entity node among all nodes
        all_nodes = self.get_all_nodes(graph_id)
        entity_node = None
        for node in all_nodes:
            if node.name.lower() == entity_name.lower():
                entity_node = node
                break
        
        related_edges = []
        if entity_node:
            # Pass graph_id
            related_edges = self.get_node_edges(graph_id, entity_node.uuid)
        
        return {
            "entity_name": entity_name,
            "entity_info": entity_node.to_dict() if entity_node else None,
            "related_facts": search_result.facts,
            "related_edges": [e.to_dict() for e in related_edges],
            "total_relations": len(related_edges)
        }
    
    def get_graph_statistics(self, graph_id: str) -> Dict[str, Any]:
        """Get basic statistics for a graph."""
        logger.info(f"Getting statistics for graph {graph_id}...")
        
        nodes = self.get_all_nodes(graph_id)
        edges = self.get_all_edges(graph_id)
        
        # Entity type distribution
        entity_types = {}
        for node in nodes:
            for label in node.labels:
                if label not in ["Entity", "Node"]:
                    entity_types[label] = entity_types.get(label, 0) + 1
        
        # Relation type distribution
        relation_types = {}
        for edge in edges:
            relation_types[edge.name] = relation_types.get(edge.name, 0) + 1
        
        return {
            "graph_id": graph_id,
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "entity_types": entity_types,
            "relation_types": relation_types
        }
    
    def get_simulation_context(
        self, 
        graph_id: str,
        simulation_requirement: str,
        limit: int = 30
    ) -> Dict[str, Any]:
        """Get simulation-related context by aggregating retrieval and graph stats."""
        logger.info(f"Getting simulation context: {simulation_requirement[:50]}...")
        
        # Search facts related to the simulation requirement
        search_result = self.search_graph(
            graph_id=graph_id,
            query=simulation_requirement,
            limit=limit
        )
        
        # Graph statistics
        stats = self.get_graph_statistics(graph_id)
        
        # All nodes
        all_nodes = self.get_all_nodes(graph_id)
        
        # Filter entities that have a custom type (non-generic Entity nodes)
        entities = []
        for node in all_nodes:
            custom_labels = [l for l in node.labels if l not in ["Entity", "Node"]]
            if custom_labels:
                entities.append({
                    "name": node.name,
                    "type": custom_labels[0],
                    "summary": node.summary
                })
        
        return {
            "simulation_requirement": simulation_requirement,
            "related_facts": search_result.facts,
            "graph_statistics": stats,
            "entities": entities[:limit],  # Limit
            "total_entities": len(entities)
        }
    
    # ========== Core retrieval tools (optimized) ==========
    
    def insight_forge(
        self,
        graph_id: str,
        query: str,
        simulation_requirement: str,
        report_context: str = "",
        max_sub_queries: int = 5
    ) -> InsightForgeResult:
        """InsightForge - deep insight retrieval.

        Automatically decomposes the question into sub-questions and retrieves facts/entities/relations.
        """
        logger.info(f"InsightForge deep retrieval: {query[:50]}...")
        
        result = InsightForgeResult(
            query=query,
            simulation_requirement=simulation_requirement,
            sub_queries=[]
        )
        
        # Step 1: Use LLM to generate sub-questions
        sub_queries = self._generate_sub_queries(
            query=query,
            simulation_requirement=simulation_requirement,
            report_context=report_context,
            max_queries=max_sub_queries
        )
        result.sub_queries = sub_queries
        logger.info(f"Generated {len(sub_queries)} sub-questions")
        
        # Step 2: Semantic search for each sub-question
        all_facts = []
        all_edges = []
        seen_facts = set()
        
        for sub_query in sub_queries:
            search_result = self.search_graph(
                graph_id=graph_id,
                query=sub_query,
                limit=15,
                scope="edges"
            )
            
            for fact in search_result.facts:
                if fact not in seen_facts:
                    all_facts.append(fact)
                    seen_facts.add(fact)
            
            all_edges.extend(search_result.edges)
        
        # Also search the original question
        main_search = self.search_graph(
            graph_id=graph_id,
            query=query,
            limit=20,
            scope="edges"
        )
        for fact in main_search.facts:
            if fact not in seen_facts:
                all_facts.append(fact)
                seen_facts.add(fact)
        
        result.semantic_facts = all_facts
        result.total_facts = len(all_facts)
        
        # Step 3: Extract related entity UUIDs from edges and fetch only those nodes
        entity_uuids = set()
        for edge_data in all_edges:
            if isinstance(edge_data, dict):
                source_uuid = edge_data.get('source_node_uuid', '')
                target_uuid = edge_data.get('target_node_uuid', '')
                if source_uuid:
                    entity_uuids.add(source_uuid)
                if target_uuid:
                    entity_uuids.add(target_uuid)
        
        # Fetch details for all related entities (no truncation)
        entity_insights = []
        node_map = {}  # Used for relationship-chain construction
        
        for uuid in list(entity_uuids):  # Process all entities, no truncation
            if not uuid:
                continue
            try:
                # Fetch each related node
                node = self.get_node_detail(uuid)
                if node:
                    node_map[uuid] = node
                    entity_type = next((l for l in node.labels if l not in ["Entity", "Node"]), "Entity")
                    
                    # Gather all facts related to this entity (no truncation)
                    related_facts = [
                        f for f in all_facts 
                        if node.name.lower() in f.lower()
                    ]
                    
                    entity_insights.append({
                        "uuid": node.uuid,
                        "name": node.name,
                        "type": entity_type,
                        "summary": node.summary,
                        "related_facts": related_facts  # No truncation
                    })
            except Exception as e:
                logger.debug(f"Failed to fetch node {uuid}: {e}")
                continue
        
        result.entity_insights = entity_insights
        result.total_entities = len(entity_insights)
        
        # Step 4: Build relationship chains (no truncation)
        relationship_chains = []
        for edge_data in all_edges:  # Process all edges, no truncation
            if isinstance(edge_data, dict):
                source_uuid = edge_data.get('source_node_uuid', '')
                target_uuid = edge_data.get('target_node_uuid', '')
                relation_name = edge_data.get('name', '')
                
                source_name = node_map.get(source_uuid, NodeInfo('', '', [], '', {})).name or source_uuid[:8]
                target_name = node_map.get(target_uuid, NodeInfo('', '', [], '', {})).name or target_uuid[:8]
                
                chain = f"{source_name} --[{relation_name}]--> {target_name}"
                if chain not in relationship_chains:
                    relationship_chains.append(chain)
        
        result.relationship_chains = relationship_chains
        result.total_relationships = len(relationship_chains)
        
        logger.info(
            f"InsightForge complete: {result.total_facts} facts, {result.total_entities} entities, "
            f"{result.total_relationships} relationships"
        )
        return result
    
    def _generate_sub_queries(
        self,
        query: str,
        simulation_requirement: str,
        report_context: str = "",
        max_queries: int = 5
    ) -> List[str]:
        """Use LLM to generate sub-questions."""
        system_prompt = """You are a professional question analyst. Break a complex question into multiple sub-questions that can be independently observed in the simulation.

    Requirements:
    1. Each sub-question should be specific enough to find relevant agent behaviors or events
    2. Cover different dimensions of the original question (who/what/why/how/when/where)
    3. Keep it relevant to the simulation scenario
    4. Return JSON: {"sub_queries": ["sub question 1", "sub question 2", ...]}"""

        user_prompt = f"""Simulation background:
    {simulation_requirement}

    {f"Report context: {report_context[:500]}" if report_context else ""}

    Please decompose the following question into {max_queries} sub-questions:
{query}

    Return the sub-question list in JSON."""

        try:
            response = self.llm.chat_json(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3
            )
            
            sub_queries = response.get("sub_queries", [])
            # Ensure it is a list of strings
            return [str(sq) for sq in sub_queries[:max_queries]]
            
        except Exception as e:
            logger.warning(f"Failed to generate sub-questions: {str(e)}; using defaults")
            # Fallback: simple variants of the original question
            return [
                query,
                f"Who are the key participants in: {query}",
                f"What are the causes and impacts of: {query}",
                f"How did the situation evolve regarding: {query}",
            ][:max_queries]
    
    def panorama_search(
        self,
        graph_id: str,
        query: str,
        include_expired: bool = True,
        limit: int = 50
    ) -> PanoramaResult:
        """PanoramaSearch - broad retrieval.

        Produces a panorama view including historical/expired information.
        """
        logger.info(f"PanoramaSearch broad retrieval: {query[:50]}...")
        
        result = PanoramaResult(query=query)
        
        # Fetch all nodes
        all_nodes = self.get_all_nodes(graph_id)
        node_map = {n.uuid: n for n in all_nodes}
        result.all_nodes = all_nodes
        result.total_nodes = len(all_nodes)
        
        # Fetch all edges (with temporal info)
        all_edges = self.get_all_edges(graph_id, include_temporal=True)
        result.all_edges = all_edges
        result.total_edges = len(all_edges)
        
        # Categorize facts
        active_facts = []
        historical_facts = []
        
        for edge in all_edges:
            if not edge.fact:
                continue
            
            # Add entity names for context
            source_name = node_map.get(edge.source_node_uuid, NodeInfo('', '', [], '', {})).name or edge.source_node_uuid[:8]
            target_name = node_map.get(edge.target_node_uuid, NodeInfo('', '', [], '', {})).name or edge.target_node_uuid[:8]
            
            # Determine whether this is historical
            is_historical = edge.is_expired or edge.is_invalid
            
            if is_historical:
                # Historical/expired fact with time markers
                valid_at = edge.valid_at or "unknown"
                invalid_at = edge.invalid_at or edge.expired_at or "unknown"
                fact_with_time = f"[{valid_at} - {invalid_at}] {edge.fact}"
                historical_facts.append(fact_with_time)
            else:
                # Active fact
                active_facts.append(edge.fact)
        
        # Sort by relevance
        query_lower = query.lower()
        keywords = [w.strip() for w in query_lower.replace(',', ' ').replace('，', ' ').split() if len(w.strip()) > 1]
        
        def relevance_score(fact: str) -> int:
            fact_lower = fact.lower()
            score = 0
            if query_lower in fact_lower:
                score += 100
            for kw in keywords:
                if kw in fact_lower:
                    score += 10
            return score
        
        # Sort and limit
        active_facts.sort(key=relevance_score, reverse=True)
        historical_facts.sort(key=relevance_score, reverse=True)
        
        result.active_facts = active_facts[:limit]
        result.historical_facts = historical_facts[:limit] if include_expired else []
        result.active_count = len(active_facts)
        result.historical_count = len(historical_facts)
        
        logger.info(
            f"PanoramaSearch complete: {result.active_count} active, {result.historical_count} historical"
        )
        return result
    
    def quick_search(
        self,
        graph_id: str,
        query: str,
        limit: int = 10
    ) -> SearchResult:
        """QuickSearch - lightweight retrieval."""
        logger.info(f"QuickSearch: {query[:50]}...")
        
        # Use existing search_graph
        result = self.search_graph(
            graph_id=graph_id,
            query=query,
            limit=limit,
            scope="edges"
        )
        
        logger.info(f"QuickSearch complete: {result.total_count} results")
        return result
    
    def interview_agents(
        self,
        simulation_id: str,
        interview_requirement: str,
        simulation_requirement: str = "",
        max_agents: int = 5,
        custom_questions: List[str] = None
    ) -> InterviewResult:
        """InterviewAgents - deep interviews via the real OASIS interview API.

        This requires the simulation environment to be running.
        """
        from .simulation_runner import SimulationRunner
        
        logger.info(f"InterviewAgents (real API): {interview_requirement[:50]}...")
        
        result = InterviewResult(
            interview_topic=interview_requirement,
            interview_questions=custom_questions or []
        )
        
        # Step 1: Load profiles
        profiles = self._load_agent_profiles(simulation_id)
        
        if not profiles:
            logger.warning(f"No agent profiles found for simulation {simulation_id}")
            result.summary = "No agent profiles were found for interview."
            return result
        
        result.total_agents = len(profiles)
        logger.info(f"Loaded {len(profiles)} agent profiles")
        
        # Step 2: Use LLM to select agents to interview (returns agent indices)
        selected_agents, selected_indices, selection_reasoning = self._select_agents_for_interview(
            profiles=profiles,
            interview_requirement=interview_requirement,
            simulation_requirement=simulation_requirement,
            max_agents=max_agents
        )
        
        result.selected_agents = selected_agents
        result.selection_reasoning = selection_reasoning
        logger.info(f"Selected {len(selected_agents)} agents for interview: {selected_indices}")
        
        # Step 3: Generate interview questions (if not provided)
        if not result.interview_questions:
            result.interview_questions = self._generate_interview_questions(
                interview_requirement=interview_requirement,
                simulation_requirement=simulation_requirement,
                selected_agents=selected_agents
            )
            logger.info(f"Generated {len(result.interview_questions)} interview questions")
        
        # Merge questions into a single prompt
        combined_prompt = "\n".join([f"{i+1}. {q}" for i, q in enumerate(result.interview_questions)])
        
        # Add a prefix to constrain the response format
        INTERVIEW_PROMPT_PREFIX = (
            "You are being interviewed. Based on your persona and your past memories/actions, "
            "answer the following questions directly in plain text.\n"
            "Response requirements:\n"
            "1. Answer in natural language; do not call any tools\n"
            "2. Do not return JSON or tool-call formats\n"
            "3. Do not use Markdown headings (e.g., #, ##, ###)\n"
            "4. Answer each numbered question; prefix each answer with 'Question X:'\n"
            "5. Separate answers with a blank line\n"
            "6. Provide substantive content; at least 2-3 sentences per question\n\n"
        )
        optimized_prompt = f"{INTERVIEW_PROMPT_PREFIX}{combined_prompt}"
        
        # Step 4: Call the real interview API (dual-platform by default)
        try:
            # Build batch request list
            interviews_request = []
            for agent_idx in selected_indices:
                interviews_request.append({
                    "agent_id": agent_idx,
                    "prompt": optimized_prompt  # Use optimized prompt
                    # No platform specified => API interviews on both Twitter and Reddit
                })
            
            logger.info(f"Calling batch interview API (dual-platform): {len(interviews_request)} agents")
            
            # Call SimulationRunner batch interview method
            api_result = SimulationRunner.interview_agents_batch(
                simulation_id=simulation_id,
                interviews=interviews_request,
                platform=None,  # Dual-platform
                timeout=180.0   # Dual-platform needs longer timeout
            )
            
            logger.info(
                f"Interview API returned: {api_result.get('interviews_count', 0)} results, "
                f"success={api_result.get('success')}"
            )
            
            # Check whether API call succeeded
            if not api_result.get("success", False):
                error_msg = api_result.get("error", "Unknown error")
                logger.warning(f"Interview API failed: {error_msg}")
                result.summary = (
                    f"Interview API call failed: {error_msg}. "
                    "Please check whether the OASIS simulation environment is running."
                )
                return result
            
            # Step 5: Parse results and build AgentInterview objects
            # Dual-platform response format: {"twitter_0": {...}, "reddit_0": {...}, ...}
            api_data = api_result.get("result", {})
            results_dict = api_data.get("results", {}) if isinstance(api_data, dict) else {}
            
            for i, agent_idx in enumerate(selected_indices):
                agent = selected_agents[i]
                agent_name = agent.get("realname", agent.get("username", f"Agent_{agent_idx}"))
                agent_role = agent.get("profession", "Unknown")
                agent_bio = agent.get("bio", "")
                
                # Get results from both platforms
                twitter_result = results_dict.get(f"twitter_{agent_idx}", {})
                reddit_result = results_dict.get(f"reddit_{agent_idx}", {})
                
                twitter_response = twitter_result.get("response", "")
                reddit_response = reddit_result.get("response", "")

                # Clean possible tool-call JSON wrappers
                twitter_response = self._clean_tool_call_response(twitter_response)
                reddit_response = self._clean_tool_call_response(reddit_response)

                # Always emit dual-platform markers
                twitter_text = twitter_response if twitter_response else "(no response from this platform)"
                reddit_text = reddit_response if reddit_response else "(no response from this platform)"
                response_text = f"[Twitter Response]\n{twitter_text}\n\n[Reddit Response]\n{reddit_text}"

                # Extract key quotes (from both platforms)
                import re
                combined_responses = f"{twitter_response} {reddit_response}"

                # Clean response text: remove headings/markup/numbering
                clean_text = re.sub(r'#{1,6}\s+', '', combined_responses)
                clean_text = re.sub(r'\{[^}]*tool_name[^}]*\}', '', clean_text)
                clean_text = re.sub(r'[*_`|>~\-]{2,}', '', clean_text)
                clean_text = re.sub(
                    r'(?:Question|Q|\u95ee\u9898)\s*\d+\s*[：:]\s*',
                    '',
                    clean_text,
                    flags=re.IGNORECASE,
                )
                clean_text = re.sub(r'【[^】]+】', '', clean_text)
                clean_text = re.sub(r'\[[^\]]+\]', '', clean_text)

                # Strategy 1 (primary): extract meaningful full sentences
                sentences = re.split(r'[.!?\u3002\uff01\uff1f]', clean_text)
                meaningful = [
                    s.strip() for s in sentences
                    if 20 <= len(s.strip()) <= 150
                    and not re.match(r'^[\s\W，,；;：:、]+', s.strip())
                    and not s.strip().startswith(('{', 'Question', '\u95ee\u9898'))
                ]
                meaningful.sort(key=len, reverse=True)
                key_quotes = [s + "." for s in meaningful[:3]]

                # Strategy 2 (fallback): paired quotes blocks
                if not key_quotes:
                    paired = re.findall(r'\u201c([^\u201c\u201d]{15,100})\u201d', clean_text)
                    paired += re.findall(r'\u300c([^\u300c\u300d]{15,100})\u300d', clean_text)
                    key_quotes = [q for q in paired if not re.match(r'^[，,；;：:、]', q)][:3]
                
                interview = AgentInterview(
                    agent_name=agent_name,
                    agent_role=agent_role,
                    agent_bio=agent_bio[:1000],  # Allow longer bios
                    question=combined_prompt,
                    response=response_text,
                    key_quotes=key_quotes[:5]
                )
                result.interviews.append(interview)
            
            result.interviewed_count = len(result.interviews)
            
        except ValueError as e:
            # Simulation environment likely not running
            logger.warning(f"Interview API call failed (env not running?): {e}")
            result.summary = (
                f"Interview failed: {str(e)}. "
                "The simulation environment may be closed; ensure OASIS is running."
            )
            return result
        except Exception as e:
            logger.error(f"Interview API error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            result.summary = f"An error occurred during interview: {str(e)}"
            return result
        
        # Step 6: Generate summary
        if result.interviews:
            result.summary = self._generate_interview_summary(
                interviews=result.interviews,
                interview_requirement=interview_requirement
            )
        
        logger.info(
            f"InterviewAgents complete: interviewed {result.interviewed_count} agents (dual-platform)"
        )
        return result
    
    @staticmethod
    def _clean_tool_call_response(response: str) -> str:
        """Strip JSON tool-call wrappers from agent responses and return the actual content."""
        if not response or not response.strip().startswith('{'):
            return response
        text = response.strip()
        if 'tool_name' not in text[:80]:
            return response
        import re as _re
        try:
            data = json.loads(text)
            if isinstance(data, dict) and 'arguments' in data:
                for key in ('content', 'text', 'body', 'message', 'reply'):
                    if key in data['arguments']:
                        return str(data['arguments'][key])
        except (json.JSONDecodeError, KeyError, TypeError):
            match = _re.search(r'"content"\s*:\s*"((?:[^"\\]|\\.)*)"', text)
            if match:
                return match.group(1).replace('\\n', '\n').replace('\\"', '"')
        return response

    def _load_agent_profiles(self, simulation_id: str) -> List[Dict[str, Any]]:
        """Load agent profile files for a simulation."""
        import os
        import csv
        
        # Build profile path
        sim_dir = os.path.join(
            os.path.dirname(__file__), 
            f'../../uploads/simulations/{simulation_id}'
        )
        
        profiles = []
        
        # Prefer Reddit JSON format
        reddit_profile_path = os.path.join(sim_dir, "reddit_profiles.json")
        if os.path.exists(reddit_profile_path):
            try:
                with open(reddit_profile_path, 'r', encoding='utf-8') as f:
                    profiles = json.load(f)
                logger.info(f"Loaded {len(profiles)} profiles from reddit_profiles.json")
                return profiles
            except Exception as e:
                logger.warning(f"Failed to read reddit_profiles.json: {e}")
        
            # Try Twitter CSV format
        twitter_profile_path = os.path.join(sim_dir, "twitter_profiles.csv")
        if os.path.exists(twitter_profile_path):
            try:
                with open(twitter_profile_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Normalize CSV format into a unified schema
                        profiles.append({
                            "realname": row.get("name", ""),
                            "username": row.get("username", ""),
                            "bio": row.get("description", ""),
                            "persona": row.get("user_char", ""),
                            "profession": "Unknown"
                        })
                logger.info(f"Loaded {len(profiles)} profiles from twitter_profiles.csv")
                return profiles
            except Exception as e:
                logger.warning(f"Failed to read twitter_profiles.csv: {e}")
        
        return profiles
    
    def _select_agents_for_interview(
        self,
        profiles: List[Dict[str, Any]],
        interview_requirement: str,
        simulation_requirement: str,
        max_agents: int
    ) -> tuple:
        """Use LLM to select which agents to interview."""
        
        # Build agent summary list
        agent_summaries = []
        for i, profile in enumerate(profiles):
            summary = {
                "index": i,
                "name": profile.get("realname", profile.get("username", f"Agent_{i}")),
                "profession": profile.get("profession", "Unknown"),
                "bio": profile.get("bio", "")[:200],
                "interested_topics": profile.get("interested_topics", [])
            }
            agent_summaries.append(summary)
        
        system_prompt = """You are a professional interview planner. Based on the interview requirement, select the most suitable agents from a simulated agent list.

    Selection criteria:
    1. The agent's identity/profession is relevant to the interview topic
    2. The agent may provide unique or valuable perspectives
    3. Choose diverse viewpoints (supporting/opposing/neutral/experts, etc.)
    4. Prefer roles directly involved in the event

    Return JSON:
    {
      "selected_indices": [list of selected agent indices],
      "reasoning": "why you chose them"
    }"""

        user_prompt = f"""Interview requirement:
    {interview_requirement}

    Simulation background:
    {simulation_requirement if simulation_requirement else "Not provided"}

    Candidate agents (total {len(agent_summaries)}):
    {json.dumps(agent_summaries, ensure_ascii=False, indent=2)}

    Select up to {max_agents} agents and explain your reasoning."""

        try:
            response = self.llm.chat_json(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3
            )
            
            selected_indices = response.get("selected_indices", [])[:max_agents]
            reasoning = response.get("reasoning", "Auto-selected based on relevance")
            
            # Fetch selected agent info
            selected_agents = []
            valid_indices = []
            for idx in selected_indices:
                if 0 <= idx < len(profiles):
                    selected_agents.append(profiles[idx])
                    valid_indices.append(idx)
            
            return selected_agents, valid_indices, reasoning
            
        except Exception as e:
            logger.warning(f"LLM selection failed; using default selection: {e}")
            # Fallback: choose first N
            selected = profiles[:max_agents]
            indices = list(range(min(max_agents, len(profiles))))
            return selected, indices, "Default selection strategy"
    
    def _generate_interview_questions(
        self,
        interview_requirement: str,
        simulation_requirement: str,
        selected_agents: List[Dict[str, Any]]
    ) -> List[str]:
        """Use LLM to generate interview questions."""
        
        agent_roles = [a.get("profession", "Unknown") for a in selected_agents]
        
        system_prompt = """You are a professional journalist/interviewer. Based on the interview requirement, generate 3-5 deep interview questions.

    Requirements:
    1. Open-ended questions that encourage detailed answers
    2. Questions that different roles could answer differently
    3. Cover facts, opinions, and feelings
    4. Natural language, like a real interview
    5. Keep each question within ~50 words
    6. Ask directly; do not include background prefaces

    Return JSON: {"questions": ["question 1", "question 2", ...]}"""

        user_prompt = f"""Interview requirement: {interview_requirement}

    Simulation background: {simulation_requirement if simulation_requirement else "Not provided"}

    Interviewee roles: {', '.join(agent_roles)}

    Generate 3-5 interview questions."""

        try:
            response = self.llm.chat_json(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.5
            )
            
            return response.get("questions", [f"What is your view on {interview_requirement}?"])
            
        except Exception as e:
            logger.warning(f"Failed to generate interview questions: {e}")
            return [
                f"What is your perspective on {interview_requirement}?",
                "How does this affect you or the group you represent?",
                "What do you think should be done to resolve or improve the situation?",
            ]
    
    def _generate_interview_summary(
        self,
        interviews: List[AgentInterview],
        interview_requirement: str
    ) -> str:
        """Generate interview summary."""
        
        if not interviews:
            return "No interviews were completed."
        
        # Gather interview content
        interview_texts = []
        for interview in interviews:
            interview_texts.append(f"[{interview.agent_name} ({interview.agent_role})]\n{interview.response[:500]}")
        
        system_prompt = """You are a professional news editor. Based on multiple interviewees' answers, generate an interview summary.

Requirements:
1. Extract main viewpoints from each side
2. Highlight consensus and disagreements
3. Surface valuable quotes
4. Stay objective and neutral
5. Keep it within about 1000 words

Format constraints (must follow):
- Use plain text paragraphs separated by blank lines
- Do not use Markdown headings (e.g., #, ##, ###)
- Do not use horizontal rules (e.g., --- or ***)
- Use standard quotes when quoting interviewees
"""

        user_prompt = f"""Interview topic: {interview_requirement}

Interview content:
{''.join(interview_texts)}

Generate the interview summary."""

        try:
            summary = self.llm.chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,
                max_tokens=800
            )
            return summary
            
        except Exception as e:
            logger.warning(f"Failed to generate interview summary: {e}")
            # Fallback: simple concatenation
            return f"Interviewed {len(interviews)} participants, including: " + ", ".join([i.agent_name for i in interviews])
