"""LLM client wrapper using the OpenAI-compatible API format."""

import json
import math
import re
from typing import Optional, Dict, Any, List
from openai import OpenAI

from ..config import Config


def _sanitize_string_for_json(value: str) -> str:
    """Remove characters that frequently break upstream JSON request parsing."""
    sanitized_chars = []
    for ch in value:
        codepoint = ord(ch)

        # Keep common whitespace, drop other ASCII control chars.
        if codepoint < 0x20:
            if ch in ("\n", "\r", "\t"):
                sanitized_chars.append(ch)
            continue

        # Drop lone surrogate code points; they are invalid in JSON/UTF-8 payloads.
        if 0xD800 <= codepoint <= 0xDFFF:
            continue

        sanitized_chars.append(ch)

    return "".join(sanitized_chars)


def sanitize_llm_payload(value: Any) -> Any:
    """Recursively sanitize data before sending it to the LLM API."""
    if isinstance(value, str):
        return _sanitize_string_for_json(value)

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, dict):
        return {
            str(sanitize_llm_payload(key)): sanitize_llm_payload(val)
            for key, val in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [sanitize_llm_payload(item) for item in value]

    return value


def is_unrecoverable_llm_request_error(error: Exception) -> bool:
    """Detect request-shape errors that should fail fast instead of retrying."""
    message = str(error).lower()
    fatal_markers = [
        "could not parse the json body",
        "not valid json",
        "invalid json",
        "malformed json",
    ]
    return any(marker in message for marker in fatal_markers)


class LLMClient:
    """LLM client."""

    @staticmethod
    def _uses_max_completion_tokens(model: Optional[str]) -> bool:
        """Return whether the model expects `max_completion_tokens`."""
        if not model:
            return False

        normalized = model.lower()
        return normalized.startswith(("gpt-5", "o1", "o3", "o4"))
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ):
        self.api_key = api_key or Config.LLM_API_KEY
        self.base_url = base_url or Config.LLM_BASE_URL
        self.model = model or Config.LLM_MODEL_NAME
        self.timeout_seconds = timeout_seconds or Config.LLM_TIMEOUT_SECONDS
        
        if not self.api_key:
            raise ValueError("LLM_API_KEY is not configured")
        
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.timeout_seconds,
        )
    
    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 4096,
        response_format: Optional[Dict] = None
    ) -> str:
        """
        Send a chat request.

        Args:
            messages: Message list.
            temperature: Sampling temperature.
            max_tokens: Maximum token count.
            response_format: Response format, such as JSON mode.

        Returns:
            Model response text.
        """
        kwargs = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "timeout": self.timeout_seconds,
        }

        token_param = "max_completion_tokens" if self._uses_max_completion_tokens(self.model) else "max_tokens"
        kwargs[token_param] = max_tokens

        if response_format:
            kwargs["response_format"] = response_format

        kwargs = sanitize_llm_payload(kwargs)
        
        response = self.client.chat.completions.create(**kwargs)
        content = response.choices[0].message.content
        # Some models include <think> blocks in content; strip them out.
        content = re.sub(r'<think>[\s\S]*?</think>', '', content).strip()
        return content
    
    def chat_json(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 4096
    ) -> Dict[str, Any]:
        """
        Send a chat request and return JSON.

        Args:
            messages: Message list.
            temperature: Sampling temperature.
            max_tokens: Maximum token count.

        Returns:
            Parsed JSON object.
        """
        response = self.chat(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"}
        )
        # Strip markdown code fences.
        cleaned_response = response.strip()
        cleaned_response = re.sub(r'^```(?:json)?\s*\n?', '', cleaned_response, flags=re.IGNORECASE)
        cleaned_response = re.sub(r'\n?```\s*$', '', cleaned_response)
        cleaned_response = cleaned_response.strip()

        try:
            return json.loads(cleaned_response)
        except json.JSONDecodeError:
            raise ValueError(f"LLM returned invalid JSON: {cleaned_response}")

