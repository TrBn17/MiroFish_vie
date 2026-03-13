"""
Dich vu sinh ontology.
API 1: phan tich noi dung van ban de tao dinh nghia loai thuc the va quan he phu hop cho mo phong xa hoi.
"""

import json
from typing import Dict, Any, List, Optional
from ..utils.llm_client import LLMClient


# System prompt de sinh ontology
ONTOLOGY_SYSTEM_PROMPT = """
You are an **ontology design expert for knowledge graphs**. Your task is to **analyze the provided text content and simulation requirements**, then design appropriate **entity types** and **relationship types** for **simulating public opinion on social media**.

**Important: You must output valid JSON only. Do not output any other content.**

# Core Task Context

We are building a **social media public opinion simulation system**. In this system:

* Each entity represents a **social media account or actor capable of making statements online**.
* Entities can **interact, influence each other, repost, comment, and respond**.
* We need to **simulate reactions from different actors and the spread of information during a public opinion event**.

Therefore, **entities must represent real-world actors that can speak or interact on social media**.

### Possible entities include:

* Specific individuals (public figures, involved parties, KOLs, experts, ordinary users)
* Companies / businesses (including official accounts)
* Organizations (universities, associations, NGOs, unions, etc.)
* Government agencies / regulatory bodies
* Media organizations (newspapers, TV stations, news sites, self-media)
* Social media platforms themselves
* Representatives of specific groups (e.g., alumni associations, fandom groups, advocacy groups)

### Not allowed as entities:

* Abstract concepts (e.g., “public opinion”, “emotion”, “trend”)
* Topics (e.g., “academic integrity”, “education reform”)
* Positions or stances (e.g., “supporters”, “opponents”)

---

# Output Format

Output **JSON** using the following structure:

```json
{
  "entity_types": [
    {
      "name": "EntityTypeName (English, PascalCase)",
      "description": "Short description (English, max 100 characters)",
      "attributes": [
        {
          "name": "attribute_name (snake_case)",
          "type": "text",
          "description": "Attribute description"
        }
      ],
      "examples": ["Example entity 1", "Example entity 2"]
    }
  ],
  "edge_types": [
    {
      "name": "RELATIONSHIP_TYPE (English, UPPER_SNAKE_CASE)",
      "description": "Short description (English, max 100 characters)",
      "source_targets": [
        {
          "source": "Source entity type",
          "target": "Target entity type"
        }
      ],
      "attributes": []
    }
  ],
  "analysis_summary": "Brief analysis of the text content (in Vietnamese)"
}
```

---

# Design Guidelines (VERY IMPORTANT)

## 1. Entity Type Design

### Quantity Requirement

There must be **exactly 10 entity types**.

### Hierarchical Requirement (Mandatory)

The list of 10 entity types must include the following:

### A. Fallback Types (Mandatory – placed in the last two positions)

**Person**
Fallback type for **any natural individual**.
If a person does not belong to a more specific individual category, classify them as this type.

**Organization**
Fallback type for **any organization**.
If an organization does not belong to a more specific organization category, classify it as this type.

---

### B. Specific Types (8 types – designed based on the text content)

Identify the **main roles appearing in the text** and design specific entity types accordingly.

Examples:

If the text describes an **academic event**:

* `Student`
* `Professor`
* `University`

If the text describes a **business event**:

* `Company`
* `CEO`
* `Employee`

---

### Why fallback types are necessary

A text may contain characters such as:

* “an elementary school teacher”
* “a passerby”
* “some random netizen”

If there is no suitable specific type, they should be classified as **Person**.

Similarly, **small organizations or temporary groups** should be classified as **Organization**.

---

### Principles for designing specific types

* Identify **roles that appear frequently or are important in the text**.
* Each type must have **clear boundaries**, avoiding overlap.
* The **description must explain how the type differs from the fallback types**.

---

# 2. Relationship Type Design

* Quantity: **6–10 types**
* Relationships must reflect **real interactions on social media**
* `source_targets` must **cover the defined entity types**

---

# 3. Attribute Design

Each entity type should have **1–3 key attributes**.

### Forbidden attribute names

Do **not** use:

* `name`
* `uuid`
* `group_id`
* `created_at`
* `summary`

(these are **system-reserved keywords**)

---

### Recommended attributes

Examples:

* `full_name`
* `title`
* `role`
* `position`
* `location`
* `description`

---

# Reference Entity Types

### Individuals (Specific)

* Student
* Professor
* Journalist
* Celebrity
* Executive
* Official
* Lawyer
* Doctor

### Individuals (Fallback)

* Person — any individual not covered by a specific type

---

### Organizations (Specific)

* University
* Company
* GovernmentAgency
* MediaOutlet
* Hospital
* School
* NGO

### Organizations (Fallback)

* Organization — any organization not covered by a specific type

---

# Reference Relationship Types

* WORKS_FOR
* STUDIES_AT
* AFFILIATED_WITH
* REPRESENTS
* REGULATES
* REPORTS_ON
* COMMENTS_ON
* RESPONDS_TO
* SUPPORTS
* OPPOSES
* COLLABORATES_WITH
* COMPETES_WITH
"""


class OntologyGenerator:
    """
    Trinh tao ontology.
    Phan tich noi dung van ban de sinh cac loai thuc the va loai quan he.
    """
    
    def __init__(self, llm_client: Optional[LLMClient] = None):
        self.llm_client = llm_client or LLMClient()
    
    def generate(
        self,
        document_texts: List[str],
        simulation_requirement: str,
        additional_context: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Tao dinh nghia ontology.

        Args:
            document_texts: Danh sach van ban tai lieu.
            simulation_requirement: Mo ta nhu cau mo phong.
            additional_context: Ngu canh bo sung.

        Returns:
            Dinh nghia ontology (`entity_types`, `edge_types`, ...).
        """
        # Tao user message
        user_message = self._build_user_message(
            document_texts, 
            simulation_requirement,
            additional_context
        )
        
        messages = [
            {"role": "system", "content": ONTOLOGY_SYSTEM_PROMPT},
            {"role": "user", "content": user_message}
        ]
        
        # Goi LLM
        result = self.llm_client.chat_json(
            messages=messages,
            temperature=0.3,
            max_tokens=4096
        )
        
        # Kiem tra va hau xu ly
        result = self._validate_and_process(result)
        
        return result
    
    # Do dai toi da cua van ban gui cho LLM (50.000 ky tu)
    MAX_TEXT_LENGTH_FOR_LLM = 50000
    
    def _build_user_message(
        self,
        document_texts: List[str],
        simulation_requirement: str,
        additional_context: Optional[str]
    ) -> str:
        """Tao user message."""
        
        # Gop cac doan van ban
        combined_text = "\n\n---\n\n".join(document_texts)
        original_length = len(combined_text)
        
        # Neu van ban vuot qua 50.000 ky tu thi cat bot (chi anh huong noi dung gui cho LLM, khong anh huong viec xay do thi)
        if len(combined_text) > self.MAX_TEXT_LENGTH_FOR_LLM:
            combined_text = combined_text[:self.MAX_TEXT_LENGTH_FOR_LLM]
            combined_text += f"\n\n...(Van ban goc dai {original_length} ky tu, da cat {self.MAX_TEXT_LENGTH_FOR_LLM} ky tu dau de phan tich ontology)..."
        
        message = f"""## Nhu cau mo phong

{simulation_requirement}

## Noi dung tai lieu

{combined_text}
"""
        
        if additional_context:
            message += f"""
## Ghi chu bo sung

{additional_context}
"""
        
        message += """
Please base on the content above to design entity types and relationship types suitable for simulating public opinion on social media.

Mandatory rules that must be followed:

Must output exactly 10 entity types

The last 2 types must be fallback types:

Person (fallback type for individuals)

Organization (fallback type for organizations)

The first 8 types are specific types designed based on the text content.

All entity types must be real-world actors capable of speaking or interacting, and must not be abstract concepts.

Attribute names must not use placeholder keywords such as name, uuid, group_id, etc. Use alternatives like full_name, org_name, etc.
"""
        
        return message
    
    def _validate_and_process(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Kiem tra va hau xu ly ket qua."""
        
        # Dam bao cac truong bat buoc ton tai
        if "entity_types" not in result:
            result["entity_types"] = []
        if "edge_types" not in result:
            result["edge_types"] = []
        if "analysis_summary" not in result:
            result["analysis_summary"] = ""
        
        # Kiem tra loai thuc the
        for entity in result["entity_types"]:
            if "attributes" not in entity:
                entity["attributes"] = []
            if "examples" not in entity:
                entity["examples"] = []
            # Dam bao `description` khong vuot qua 100 ky tu
            if len(entity.get("description", "")) > 100:
                entity["description"] = entity["description"][:97] + "..."
        
        # Kiem tra loai quan he
        for edge in result["edge_types"]:
            if "source_targets" not in edge:
                edge["source_targets"] = []
            if "attributes" not in edge:
                edge["attributes"] = []
            if len(edge.get("description", "")) > 100:
                edge["description"] = edge["description"][:97] + "..."
        
        # Gioi han cua Zep API: toi da 10 loai thuc the tuy chinh va 10 loai canh tuy chinh
        MAX_ENTITY_TYPES = 10
        MAX_EDGE_TYPES = 10
        
        # Dinh nghia loai du phong
        person_fallback = {
            "name": "Person",
            "description": "Any individual person not fitting other specific person types.",
            "attributes": [
                {"name": "full_name", "type": "text", "description": "Full name of the person"},
                {"name": "role", "type": "text", "description": "Role or occupation"}
            ],
            "examples": ["ordinary citizen", "anonymous netizen"]
        }
        
        organization_fallback = {
            "name": "Organization",
            "description": "Any organization not fitting other specific organization types.",
            "attributes": [
                {"name": "org_name", "type": "text", "description": "Name of the organization"},
                {"name": "org_type", "type": "text", "description": "Type of organization"}
            ],
            "examples": ["small business", "community group"]
        }
        
        # Kiem tra xem da co loai du phong chua
        entity_names = {e["name"] for e in result["entity_types"]}
        has_person = "Person" in entity_names
        has_organization = "Organization" in entity_names
        
        # Cac loai du phong can bo sung
        fallbacks_to_add = []
        if not has_person:
            fallbacks_to_add.append(person_fallback)
        if not has_organization:
            fallbacks_to_add.append(organization_fallback)
        
        if fallbacks_to_add:
            current_count = len(result["entity_types"])
            needed_slots = len(fallbacks_to_add)
            
            # Neu sau khi them ma vuot qua 10 loai thi can bo bot mot so loai hien co
            if current_count + needed_slots > MAX_ENTITY_TYPES:
                # Tinh so luong can loai bo
                to_remove = current_count + needed_slots - MAX_ENTITY_TYPES
                # Xoa tu cuoi danh sach (giu lai cac loai cu the quan trong hon o phia truoc)
                result["entity_types"] = result["entity_types"][:-to_remove]
            
            # Them cac loai du phong
            result["entity_types"].extend(fallbacks_to_add)
        
        # Dam bao cuoi cung khong vuot qua gioi han (phong ve bo sung)
        if len(result["entity_types"]) > MAX_ENTITY_TYPES:
            result["entity_types"] = result["entity_types"][:MAX_ENTITY_TYPES]
        
        if len(result["edge_types"]) > MAX_EDGE_TYPES:
            result["edge_types"] = result["edge_types"][:MAX_EDGE_TYPES]
        
        return result
    
    def generate_python_code(self, ontology: Dict[str, Any]) -> str:
        """
        Chuyen dinh nghia ontology thanh ma Python (tuong tu `ontology.py`).

        Args:
            ontology: Dinh nghia ontology.

        Returns:
            Chuoi ma Python.
        """
        code_lines = [
            '"""',
            'Dinh nghia loai thuc the tuy chinh',
            'Duoc MiroFish tu dong tao de phuc vu mo phong du luan xa hoi',
            '"""',
            '',
            'from pydantic import Field',
            'from zep_cloud.external_clients.ontology import EntityModel, EntityText, EdgeModel',
            '',
            '',
            '# ============== Dinh nghia loai thuc the ==============',
            '',
        ]
        
        # Tao cac loai thuc the
        for entity in ontology.get("entity_types", []):
            name = entity["name"]
            desc = entity.get("description", f"A {name} entity.")
            
            code_lines.append(f'class {name}(EntityModel):')
            code_lines.append(f'    """{desc}"""')
            
            attrs = entity.get("attributes", [])
            if attrs:
                for attr in attrs:
                    attr_name = attr["name"]
                    attr_desc = attr.get("description", attr_name)
                    code_lines.append(f'    {attr_name}: EntityText = Field(')
                    code_lines.append(f'        description="{attr_desc}",')
                    code_lines.append(f'        default=None')
                    code_lines.append(f'    )')
            else:
                code_lines.append('    pass')
            
            code_lines.append('')
            code_lines.append('')
        
        code_lines.append('# ============== Dinh nghia loai quan he ==============')
        code_lines.append('')
        
        # Tao cac loai quan he
        for edge in ontology.get("edge_types", []):
            name = edge["name"]
            # Chuyen sang ten lop PascalCase
            class_name = ''.join(word.capitalize() for word in name.split('_'))
            desc = edge.get("description", f"A {name} relationship.")
            
            code_lines.append(f'class {class_name}(EdgeModel):')
            code_lines.append(f'    """{desc}"""')
            
            attrs = edge.get("attributes", [])
            if attrs:
                for attr in attrs:
                    attr_name = attr["name"]
                    attr_desc = attr.get("description", attr_name)
                    code_lines.append(f'    {attr_name}: EntityText = Field(')
                    code_lines.append(f'        description="{attr_desc}",')
                    code_lines.append(f'        default=None')
                    code_lines.append(f'    )')
            else:
                code_lines.append('    pass')
            
            code_lines.append('')
            code_lines.append('')
        
        # Tao tu dien cau hinh kieu du lieu
        code_lines.append('# ============== Cau hinh kieu du lieu ==============')
        code_lines.append('')
        code_lines.append('ENTITY_TYPES = {')
        for entity in ontology.get("entity_types", []):
            name = entity["name"]
            code_lines.append(f'    "{name}": {name},')
        code_lines.append('}')
        code_lines.append('')
        code_lines.append('EDGE_TYPES = {')
        for edge in ontology.get("edge_types", []):
            name = edge["name"]
            class_name = ''.join(word.capitalize() for word in name.split('_'))
            code_lines.append(f'    "{name}": {class_name},')
        code_lines.append('}')
        code_lines.append('')
        
        # Tao anh xa `source_targets` cho tung canh
        code_lines.append('EDGE_SOURCE_TARGETS = {')
        for edge in ontology.get("edge_types", []):
            name = edge["name"]
            source_targets = edge.get("source_targets", [])
            if source_targets:
                st_list = ', '.join([
                    f'{{"source": "{st.get("source", "Entity")}", "target": "{st.get("target", "Entity")}"}}'
                    for st in source_targets
                ])
                code_lines.append(f'    "{name}": [{st_list}],')
        code_lines.append('}')
        
        return '\n'.join(code_lines)

