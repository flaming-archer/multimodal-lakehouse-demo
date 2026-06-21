"""
LLM client — supports OpenAI-compatible and CodeBuddy native APIs.

Configure via environment variables:
  LLM_PROVIDER - "openai" (default) or "codebuddy"
  LLM_API_KEY  - API key / token (required)
  LLM_API_BASE - Base URL, OpenAI: https://api.openai.com/v1
                           CodeBuddy: http://127.0.0.1:8080
  LLM_MODEL    - Model name (OpenAI: gpt-3.5-turbo, CodeBuddy: optional)
  LLM_TIMEOUT  - Request timeout seconds, default 60
"""

import os
import re
import json
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "")  # empty = auto-detect
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_API_BASE = os.getenv("LLM_API_BASE", "https://api.openai.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-3.5-turbo")
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "120"))


def is_llm_available():
    # type: () -> bool
    """Check if LLM is available based on active provider."""
    if llm_client._provider == "codebuddy":
        import shutil
        return shutil.which("codebuddy") is not None
    return bool(LLM_API_KEY)


def _parse_json_from_text(content):
    # type: (str) -> Optional[Dict[str, Any]]
    """Extract JSON object from LLM response text."""
    text = content.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:])
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Regex fallback
    m = re.search(r"\{[\s\S]*\}", content)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass
    logger.warning("LLM JSON parse failed, raw: %s", content[:200])
    return None


class OpenAIClient:
    """Standard OpenAI-compatible chat completions."""

    def __init__(self):
        self.api_key = LLM_API_KEY
        self.api_base = LLM_API_BASE.rstrip("/")
        self.model = LLM_MODEL
        self.timeout = LLM_TIMEOUT

    def chat(self, system, user, temperature=0.3):
        # type: (str, str, float) -> Optional[str]
        import ssl
        try:
            from urllib.request import Request, urlopen
            from urllib.error import URLError, HTTPError
        except ImportError:
            logger.error("urllib not available")
            return None

        url = "{}/chat/completions".format(self.api_base)
        body = json.dumps({
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "max_tokens": 500,
        }).encode("utf-8")

        req = Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", "Bearer {}".format(self.api_key))

        try:
            ctx = ssl.create_default_context() if url.startswith("https") else None
            resp = urlopen(req, timeout=self.timeout, context=ctx)
            data = json.loads(resp.read().decode("utf-8"))
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return content.strip() if content else None
        except HTTPError as e:
            body = e.read().decode("utf-8", "ignore")[:200]
            logger.error("OpenAI HTTP %d: %s", e.code, body)
            return None
        except URLError as e:
            logger.error("OpenAI connection error: %s", e.reason)
            return None
        except Exception as e:
            logger.error("OpenAI error: %s", e)
            return None


class CodeBuddyClient:
    """CodeBuddy Code native client — uses `codebuddy -p` CLI directly.

    No HTTP server required; works from any terminal as long as `codebuddy` CLI is in PATH.
    """

    def __init__(self):
        self.timeout = LLM_TIMEOUT

    def chat(self, system, user, temperature=0.3):
        # type: (str, str, float) -> Optional[str]
        import subprocess

        prompt = system + "\n\n" + user
        try:
            proc = subprocess.run(
                ["codebuddy", "-p", "--output-format", "text",
                 "--dangerously-skip-permissions", prompt],
                capture_output=True, text=True, timeout=self.timeout
            )
            if proc.returncode != 0:
                stderr = proc.stderr[:200] if proc.stderr else ""
                logger.error("CodeBuddy CLI error (rc=%d): %s", proc.returncode, stderr)
                return None
            result = proc.stdout.strip()
            if result:
                # Strip markdown JSON fences if present
                text = result.strip()
                if text.startswith("```json"):
                    text = text[7:]
                elif text.startswith("```"):
                    text = text[3:]
                if text.endswith("```"):
                    text = text[:-3]
                return text.strip()
            return None
        except subprocess.TimeoutExpired:
            logger.error("CodeBuddy CLI timeout after %ds", self.timeout)
            return None
        except FileNotFoundError:
            logger.error("codebuddy CLI not found in PATH")
            return None
        except Exception as e:
            logger.error("CodeBuddy CLI error: %s", e)
            return None


class LLMClient:
    """Unified LLM client that auto-selects provider.
    Priority: LLM_PROVIDER env > auto-detect codebuddy CLI > openai
    """

    def __init__(self):
        import shutil
        provider = LLM_PROVIDER.lower() if LLM_PROVIDER else ""
        if provider == "codebuddy" or (not provider and shutil.which("codebuddy")):
            self._backend = CodeBuddyClient()
            self._provider = "codebuddy"
        else:
            self._backend = OpenAIClient()
            self._provider = "openai"

    def chat(self, system, user, temperature=0.3):
        # type: (str, str, float) -> Optional[str]
        return self._backend.chat(system, user, temperature)

    def analyze_transcript(self, transcript):
        # type: (str) -> Optional[Dict[str, Any]]
        """Analyze a call transcript and return structured JSON.

        Returns None if LLM is unavailable or call fails.
        """
        if not is_llm_available():
            return None

        system = (
            "你是一个运营商客服对话分析专家。"
            "分析以下通话记录，提取关键信息，只返回 JSON，不要其他内容。"
        )
        user = (
            "分析以下运营商客服通话记录，提取关键信息并以JSON格式返回：\n\n"
            "通话内容：\n{transcript}\n\n"
            "请分析并返回JSON（只返回JSON，不要其他内容）：\n"
            "{{\n"
            '    "caller_intent": "用户意图（转网/投诉/销户/降套餐/业务咨询/业务办理/其他）",\n'
            '    "switch_reason": "转网/流失原因（多个用、分隔）",\n'
            '    "sentiment": "情绪（negative/neutral/positive）",\n'
            '    "sentiment_score": 情绪分数（-1到1之间的浮点数）,\n'
            '    "risk_level": "流失风险（high/medium/low）",\n'
            '    "key_entities": {{"amount_yuan": "金额", "phone": "号码"}},\n'
            '    "suggested_action": "建议留客措施，不超过30字",\n'
            '    "summary": "30字内总结"\n'
            "}}"
        ).format(transcript=transcript)

        content = self.chat(system=system, user=user)
        if not content:
            return None

        return _parse_json_from_text(content)


# Singleton
llm_client = LLMClient()
