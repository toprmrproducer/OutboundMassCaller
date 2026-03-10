import os
from typing import Optional

from openai import AsyncOpenAI


def inject_lead_context(template: str, lead: dict, campaign: dict) -> str:
    result = template or ""
    lead = lead or {}
    campaign = campaign or {}
    result = result.replace("{{lead_name}}", lead.get("name") or "the customer")
    result = result.replace("{{lead_phone}}", lead.get("phone", ""))
    result = result.replace("{{lead_language}}", lead.get("language", "hi-IN"))
    result = result.replace("{{campaign_name}}", campaign.get("name", ""))
    result = result.replace("{{campaign_objective}}", campaign.get("objective", ""))
    custom_data = lead.get("custom_data") or {}
    for k, v in custom_data.items():
        result = result.replace(f"{{{{custom_{k}}}}}", str(v))
    return result


class AgentSimulator:
    def __init__(self, agent: dict, lead_context: Optional[dict] = None):
        self.agent = agent
        self.lead_context = lead_context or {}
        self.messages: list[dict] = []
        self.client = AsyncOpenAI(
            api_key=os.environ["OPENAI_API_KEY"],
            base_url=agent.get("llm_base_url") or None,
        )
        self._build_system_prompt(lead_context)

    def _build_system_prompt(self, lead_context):
        prompt = inject_lead_context(self.agent.get("system_prompt", ""), lead_context or {}, {})
        self.messages = [{"role": "system", "content": prompt}]
        first_line = inject_lead_context(self.agent.get("first_line", ""), lead_context or {}, {})
        if first_line:
            self.messages.append({"role": "assistant", "content": first_line})

    async def send_message(self, user_input: str, objection_handlers: list = None) -> tuple[str, bool]:
        objection_triggered = False
        self.messages.append({"role": "user", "content": user_input})

        if objection_handlers:
            lowered = user_input.lower()
            for handler in objection_handlers:
                phrases = handler.get("trigger_phrases") or []
                if any(str(p).lower() in lowered for p in phrases):
                    objection_triggered = True
                    self.messages.append(
                        {
                            "role": "system",
                            "content": (
                                "OBJECTION DETECTED. Suggested response: "
                                f"{handler.get('response', '')}. Use this as your basis."
                            ),
                        }
                    )
                    break

        resp = await self.client.chat.completions.create(
            model=self.agent.get("llm_model") or "gpt-4.1-mini",
            temperature=float(self.agent.get("llm_temperature") or 0.7),
            max_tokens=int(self.agent.get("llm_max_tokens") or 120),
            messages=self.messages,
        )
        text = (resp.choices[0].message.content or "").strip()
        self.messages.append({"role": "assistant", "content": text})
        return text, objection_triggered

    def get_conversation(self) -> list[dict]:
        return [m for m in self.messages if m.get("role") != "system"]

    def reset(self):
        self._build_system_prompt(self.lead_context)
