"""
LLM-powered voice transcript analyzer.

Uses keyword pattern matching for fast NLU analysis.
In production, add a real LLM call for ambiguous cases.

Key capabilities:
- Intent extraction (转网 / 投诉 / 销户 / 降套餐 / 业务咨询 / 业务办理)
- Reason identification with scoring
- Sentiment analysis
- Named entity recognition
- Suggested retention actions
"""

import json
import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field


# ── Intent: category -> keywords (flat list) ──

INTENT_KEYWORDS = {
    "转网/携号转网": ["转网", "携号转网", "换运营商", "换到", "携转", "转出"],
    "投诉": ["投诉", "举报", "曝光", "12315", "工信部", "赔偿"],
    "销户": ["销户", "注销", "不用了", "取消"],
    "降套餐": ["降套餐", "换便宜", "太贵", "流量不够", "用不了这么多"],
    "业务咨询": ["怎么", "如何", "咨询", "问一下", "了解一下", "查询"],
    "业务办理": ["办理", "开通", "升级", "续费", "变更"],
}

INTENT_RISK = {
    "转网/携号转网": "high",
    "投诉": "high",
    "销户": "medium",
    "降套餐": "medium",
    "业务咨询": "low",
    "业务办理": "low",
}

# ── Reason keywords (flat list per category) ──

REASON_KEYWORDS = {
    "资费过高": ["太贵", "贵了", "不划算", "性价比", "套餐费", "月租", "资费"],
    "信号差": ["信号", "没信号", "打不通", "断线", "卡", "慢", "网速", "覆盖"],
    "客服体验差": ["客服", "态度", "不理", "等待", "排队", "扯皮", "推诿"],
    "套餐不匹配": ["用不完", "不够用", "流量少", "分钟数", "短信", "不合适"],
    "竞争对手优惠": ["便宜", "送", "联通", "电信", "移动", "广电", "活动"],
    "搬家/换地区": ["搬家", "换地方", "不在", "异地", "跨省"],
    "服务不满意": ["不满意", "不好", "差", "垃圾", "坑"],
    "合约到期": ["合约", "到期", "期满"],
}

# ── Sentiment keywords ──

SENTIMENT_KEYWORDS = {
    "negative": ["差", "垃圾", "坑", "骗", "气", "无语", "不想", "算了", "不用了"],
    "neutral": ["嗯", "好", "行", "可以", "知道了"],
    "positive": ["满意", "不错", "很好", "感谢", "谢谢", "赞"],
}

# ── Retention actions ──

RETENTION_ACTIONS = {
    "资费过高": "提供专属优惠套餐 / 话费补贴 / 合约机优惠",
    "信号差": "优先处理网络覆盖问题 / 赠送信号放大器 / WiFi通话推荐",
    "客服体验差": "升级VIP客服通道 / 专属客户经理 / 问题优先处理",
    "套餐不匹配": "推荐更匹配的套餐 / 个性化定制 / 弹性计费",
    "竞争对手优惠": "匹配竞品优惠 / 老用户回馈 / 专属权益",
    "搬家/换地区": "异地业务办理支持 / 跨省套餐推荐",
    "服务不满意": "问题升级处理 / 满意度回访 / 补偿方案",
}


@dataclass
class CallAnalysis:
    call_id: str
    transcript: str
    caller_intent: str
    switch_reason: str
    sentiment: str
    sentiment_score: float
    risk_level: str
    key_entities: Dict[str, Any]
    suggested_action: str
    summary: str
    duration_seconds: int
    processed_at: str = field(default_factory=lambda: datetime.now().isoformat())


class LLMVoiceParser:
    """
    Multi-strategy voice transcript analyzer.
    Uses keyword pattern matching for fast intent/reason extraction.
    """

    @staticmethod
    def _keyword_match(text: str, kw_dict: Dict[str, List[str]]) -> List[Tuple[str, int]]:
        """
        Match text against keyword dictionary.
        Returns list of (category, score) sorted by score descending.
        """
        matches = []
        text_lower = text.lower()
        for category, keywords in kw_dict.items():
            score = 0
            for kw in keywords:
                if kw in text_lower:
                    score += 1
            if score > 0:
                matches.append((category, score))
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches

    def extract_intent(self, text: str) -> Tuple[str, str]:
        """Extract caller intent and risk level."""
        matches = self._keyword_match(text, INTENT_KEYWORDS)
        if not matches:
            return ("其他/未识别", "low")
        intent = matches[0][0]
        risk = INTENT_RISK.get(intent, "low")
        return (intent, risk)

    def extract_reasons(self, text: str) -> str:
        """Extract churn/switch reasons as delimited string."""
        matches = self._keyword_match(text, REASON_KEYWORDS)
        if not matches:
            return "未明确说明"
        reasons = [m[0] for m in matches[:2]]
        return "、".join(reasons)

    def analyze_sentiment(self, text: str) -> Tuple[str, float]:
        """Analyze caller sentiment."""
        neg = sum(1 for kw in SENTIMENT_KEYWORDS["negative"] if kw in text)
        pos = sum(1 for kw in SENTIMENT_KEYWORDS["positive"] if kw in text)
        neu = sum(1 for kw in SENTIMENT_KEYWORDS["neutral"] if kw in text)

        if neg > pos:
            score = max(-1.0, -0.3 - 0.1 * neg)
            return ("negative", score)
        elif pos > 0:
            score = min(1.0, 0.3 + 0.1 * pos)
            return ("positive", score)
        else:
            return ("neutral", 0.0)

    def extract_entities(self, text: str) -> Dict[str, Any]:
        """Extract key entities from transcript."""
        entities = {}

        # Extract amounts
        for pattern, key in [
            (r"(\d+)\s*元", "amount_yuan"),
            (r"(\d+)\s*G[BM]?", "data_gb"),
            (r"(\d+)\s*分钟", "minutes"),
            (r"(\d+)\s*个月", "months"),
        ]:
            m = re.search(pattern, text)
            if m:
                entities[key] = m.group(1)

        # Extract phone
        phone = re.search(r"1[3-9]\d{9}", text)
        if phone:
            entities["phone"] = phone.group()

        # Detect contract
        if re.search(r"合约|协议|合同", text):
            entities["has_contract"] = True
        if re.search(r"到期", text):
            entities["contract_expiring"] = True

        # Detect competitor
        for comp in ["联通", "电信", "广电"]:
            if comp in text:
                entities.setdefault("competitors", []).append(comp)

        # Detect location
        loc = re.search(r"(北京|上海|广州|深圳|杭州|成都|武汉|南京|重庆|西安|天津|苏州|长沙)", text)
        if loc:
            entities["location"] = loc.group(1)

        return entities

    def get_retention_action(self, reason: str) -> str:
        """Suggest retention action based on reason."""
        for r_key, action in RETENTION_ACTIONS.items():
            if r_key in reason:
                return action
        return "综合分析客户需求，制定个性化留客方案"

    def analyze(self, call_id: str, transcript: str) -> CallAnalysis:
        """Full analysis pipeline."""
        intent, risk = self.extract_intent(transcript)
        reasons = self.extract_reasons(transcript)
        sentiment, score = self.analyze_sentiment(transcript)
        entities = self.extract_entities(transcript)
        action = self.get_retention_action(reasons)

        summary_parts = [f"用户致电意图：【{intent}】"]
        if "转网" in intent:
            summary_parts.append(f"转网原因：{reasons}")
        summary_parts.append(f"情绪倾向：{sentiment}（{score:.2f}）")
        summary_parts.append(f"风险等级：{risk.upper()}")

        summary = "；".join(summary_parts)
        duration = max(30, len(transcript) // 3)

        return CallAnalysis(
            call_id=call_id,
            transcript=transcript,
            caller_intent=intent,
            switch_reason=reasons,
            sentiment=sentiment,
            sentiment_score=score,
            risk_level=risk,
            key_entities=entities,
            suggested_action=action,
            summary=summary,
            duration_seconds=duration,
        )

    def analyze_with_llm_prompt(self, transcript: str) -> str:
        """Generate LLM prompt for deeper analysis (for WorkBuddy AI)."""
        return f"""分析以下运营商客服通话记录，提取关键信息并以JSON格式返回：

通话内容：
{transcript}

请分析并返回JSON（只返回JSON，不要其他内容）：
{{
    "caller_intent": "用户意图（转网/投诉/销户/降套餐/业务咨询/业务办理）",
    "switch_reason": "转网/流失原因（多个用、分隔）",
    "sentiment": "情绪（negative/neutral/positive）",
    "sentiment_score": "情绪分数（-1到1之间）",
    "risk_level": "流失风险（high/medium/low）",
    "key_entities": {{"amount_yuan": "金额", "phone": "号码"}},
    "suggested_action": "建议留客措施",
    "summary": "30字内总结"
}}"""
