import { useMemo, useState } from "react";

type Message = { role: string; content: string };

type Agent = { id: string; name: string; llm_model?: string; persona?: string };

export default function AgentSimulator() {
  const [businessId, setBusinessId] = useState("");
  const [agents, setAgents] = useState<Agent[]>([]);
  const [agentId, setAgentId] = useState("");
  const [name, setName] = useState("Demo Lead");
  const [phone, setPhone] = useState("+910000000000");
  const [language, setLanguage] = useState("hi-IN");
  const [customData, setCustomData] = useState('{"product":"Premium Plan"}');
  const [sessionId, setSessionId] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);

  const selected = useMemo(() => agents.find((a) => a.id === agentId), [agents, agentId]);

  const loadAgents = async () => {
    if (!businessId) return;
    const res = await fetch(`/api/agents?business_id=${encodeURIComponent(businessId)}`);
    const data = await res.json();
    const list = (Array.isArray(data) ? data : []).map((a: any) => ({ id: String(a.id), name: a.name, llm_model: a.llm_model, persona: a.persona }));
    setAgents(list);
    if (list.length > 0) setAgentId(list[0].id);
  };

  const start = async () => {
    if (!agentId) return;
    setLoading(true);
    let parsed: any = {};
    try {
      parsed = JSON.parse(customData || "{}");
    } catch {
      parsed = {};
    }
    const res = await fetch("/api/simulator/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        agent_id: agentId,
        mock_lead: { name, phone, language, custom_data: parsed },
      }),
    });
    const body = await res.json();
    const sid = body?.data?.session_id;
    setSessionId(sid || "");
    setMessages(body?.data?.conversation || []);
    setLoading(false);
  };

  const send = async () => {
    if (!sessionId || !input.trim()) return;
    const userMsg = input.trim();
    setMessages((prev) => [...prev, { role: "user", content: userMsg }]);
    setInput("");
    setLoading(true);
    const res = await fetch(`/api/simulator/${sessionId}/message`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ content: userMsg }),
    });
    const body = await res.json();
    const response = body?.data?.response || "";
    setMessages((prev) => [...prev, { role: "assistant", content: response }]);
    setLoading(false);
  };

  const reset = async () => {
    if (!sessionId) return;
    await fetch(`/api/simulator/${sessionId}/reset`, { method: "POST" });
    setMessages([]);
  };

  const copyTranscript = () => {
    const text = messages.map((m) => `${m.role.toUpperCase()}: ${m.content}`).join("\n");
    navigator.clipboard.writeText(text);
  };

  return (
    <div className="min-h-screen bg-slate-50 p-4 md:p-6">
      <div className="mx-auto grid max-w-7xl gap-4 lg:grid-cols-3">
        <div className="rounded-xl border bg-white p-4 lg:col-span-1">
          <h2 className="mb-3 text-lg font-semibold">Agent Simulator</h2>
          <div className="space-y-2">
            <input className="w-full rounded border p-2" placeholder="Business ID" value={businessId} onChange={(e) => setBusinessId(e.target.value)} />
            <button className="rounded border px-3 py-2" onClick={loadAgents}>Load Agents</button>
            <select className="w-full rounded border p-2" value={agentId} onChange={(e) => setAgentId(e.target.value)}>
              {agents.map((a) => <option key={a.id} value={a.id}>{a.name}</option>)}
            </select>
            <input className="w-full rounded border p-2" value={name} onChange={(e) => setName(e.target.value)} placeholder="Lead Name" />
            <input className="w-full rounded border p-2" value={phone} onChange={(e) => setPhone(e.target.value)} placeholder="Lead Phone" />
            <input className="w-full rounded border p-2" value={language} onChange={(e) => setLanguage(e.target.value)} placeholder="Language" />
            <textarea className="w-full rounded border p-2" rows={4} value={customData} onChange={(e) => setCustomData(e.target.value)} />
            <button className="w-full rounded bg-indigo-600 px-3 py-2 text-white" onClick={start} disabled={loading}>Start Simulation</button>
          </div>
          {selected ? (
            <div className="mt-4 rounded border bg-slate-50 p-2 text-xs text-slate-600">
              <div>Agent: {selected.name}</div>
              <div>Model: {selected.llm_model || "gpt-4.1-mini"}</div>
              <div>Persona: {selected.persona || "professional"}</div>
            </div>
          ) : null}
        </div>

        <div className="rounded-xl border bg-white p-4 lg:col-span-2">
          <div className="mb-3 flex items-center justify-between">
            <h3 className="font-semibold">Conversation</h3>
            <div className="space-x-2">
              <button className="rounded border px-2 py-1" onClick={reset}>Reset Conversation</button>
              <button className="rounded border px-2 py-1" onClick={copyTranscript}>Copy Transcript</button>
            </div>
          </div>

          <div className="h-[420px] space-y-2 overflow-y-auto rounded border bg-slate-50 p-3">
            {messages.map((m, i) => (
              <div key={i} className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
                <div className={`max-w-[80%] rounded-lg px-3 py-2 text-sm ${m.role === "user" ? "bg-emerald-500 text-white" : "bg-white"}`}>
                  {m.content}
                </div>
              </div>
            ))}
            {loading ? <div className="text-xs text-slate-500">Agent is typing...</div> : null}
          </div>

          <div className="mt-3 flex gap-2">
            <input
              className="flex-1 rounded border p-2"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") send();
              }}
              placeholder="Type your message"
            />
            <button className="rounded bg-slate-900 px-3 py-2 text-white" onClick={send} disabled={loading || !sessionId}>Send</button>
          </div>
        </div>
      </div>
    </div>
  );
}
