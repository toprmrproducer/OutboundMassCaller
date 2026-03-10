import { useEffect, useMemo, useState } from "react";
import WizardStep from "../components/onboarding/WizardStep";

type Business = { id?: string; name: string; timezone: string; whatsapp_instance?: string };

type Trunk = {
  name: string;
  trunk_id: string;
  phone_number: string;
  number_pool: string;
  sip_uri: string;
  sip_username: string;
  sip_password: string;
};

type Agent = {
  name: string;
  stt_language: string;
  llm_model: string;
  tts_voice: string;
  system_prompt: string;
  first_line: string;
};

const TZS = ["Asia/Kolkata", "UTC", "America/New_York", "Europe/London"];

export default function Onboarding() {
  const [step, setStep] = useState(1);
  const [business, setBusiness] = useState<Business>({ name: "", timezone: "Asia/Kolkata", whatsapp_instance: "" });
  const [trunk, setTrunk] = useState<Trunk>({
    name: "",
    trunk_id: "",
    phone_number: "",
    number_pool: "",
    sip_uri: "",
    sip_username: "",
    sip_password: "",
  });
  const [agent, setAgent] = useState<Agent>({
    name: "",
    stt_language: "hi-IN",
    llm_model: "gpt-4.1-mini",
    tts_voice: "rohan",
    system_prompt: "",
    first_line: "",
  });
  const [testPhone, setTestPhone] = useState("");
  const [status, setStatus] = useState("");

  const businessId = useMemo(() => localStorage.getItem("business_id") || "", [step]);

  useEffect(() => {
    const existing = localStorage.getItem("business_id");
    if (existing) {
      fetch(`/api/onboarding/status?business_id=${encodeURIComponent(existing)}`)
        .then((r) => r.json())
        .then((d) => {
          const complete = Boolean(d?.data?.complete ?? d?.complete);
          if (complete) setStep(6);
        })
        .catch(() => {
          // ignore
        });
    }
  }, []);

  const createBusiness = async () => {
    const res = await fetch("/api/businesses", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(business),
    });
    const json = await res.json();
    const id = json?.id || json?.data?.id;
    if (id) {
      localStorage.setItem("business_id", id);
      setStep(3);
    }
  };

  const createTrunk = async () => {
    const payload = {
      business_id: businessId,
      name: trunk.name,
      trunk_id: trunk.trunk_id,
      phone_number: trunk.phone_number,
      number_pool: trunk.number_pool
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean),
    };
    await fetch("/api/sip-trunks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (trunk.sip_uri || trunk.sip_username || trunk.sip_password || trunk.phone_number) {
      await fetch("/api/config/sip", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          business_id: businessId,
          sip_uri: trunk.sip_uri,
          username: trunk.sip_username,
          password: trunk.sip_password,
          caller_id: trunk.phone_number,
        }),
      });
    }
    setStep(4);
  };

  const createAgent = async () => {
    const payload = {
      business_id: businessId,
      name: agent.name,
      stt_language: agent.stt_language,
      llm_model: agent.llm_model,
      tts_voice: agent.tts_voice,
      system_prompt: agent.system_prompt,
      first_line: agent.first_line,
    };
    const res = await fetch("/api/agents", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const json = await res.json();
    const agentId = json?.id || json?.data?.id;
    if (agentId) {
      localStorage.setItem("agent_id", agentId);
      setStep(5);
    }
  };

  const runTestCall = async () => {
    const agentId = localStorage.getItem("agent_id") || "";
    if (!agentId || !businessId || !testPhone) return;
    setStatus("Dialing test call...");
    const res = await fetch("/api/test-call", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ business_id: businessId, phone: testPhone, agent_id: agentId }),
    });
    const json = await res.json();
    setStatus(json?.error || "Test call dispatched");
    setStep(6);
  };

  return (
    <div className="min-h-screen bg-gray-50 p-4 dark:bg-gray-950">
      <div className="mx-auto max-w-3xl space-y-4">
        {step === 1 ? (
          <WizardStep
            title="Welcome to RapidXAI Outbound Caller"
            subtitle="We’ll set up business, trunk, agent and run a test call."
            actions={<button className="rounded bg-gray-900 px-4 py-2 text-white" onClick={() => setStep(2)}>Let&apos;s Get Started</button>}
          >
            <p className="text-sm text-gray-600 dark:text-gray-300">This guided setup takes 2-3 minutes.</p>
          </WizardStep>
        ) : null}

        {step === 2 ? (
          <WizardStep
            title="Business Setup"
            actions={<button className="rounded bg-gray-900 px-4 py-2 text-white" onClick={createBusiness}>Continue</button>}
          >
            <input className="w-full rounded border p-2" placeholder="Business Name" value={business.name} onChange={(e) => setBusiness((s) => ({ ...s, name: e.target.value }))} />
            <select className="w-full rounded border p-2" value={business.timezone} onChange={(e) => setBusiness((s) => ({ ...s, timezone: e.target.value }))}>
              {TZS.map((tz) => <option key={tz}>{tz}</option>)}
            </select>
            <input className="w-full rounded border p-2" placeholder="WhatsApp Instance (optional)" value={business.whatsapp_instance} onChange={(e) => setBusiness((s) => ({ ...s, whatsapp_instance: e.target.value }))} />
          </WizardStep>
        ) : null}

        {step === 3 ? (
          <WizardStep title="SIP Trunk Setup" actions={<button className="rounded bg-gray-900 px-4 py-2 text-white" onClick={createTrunk}>Continue</button>}>
            <input className="w-full rounded border p-2" placeholder="Trunk Name" value={trunk.name} onChange={(e) => setTrunk((s) => ({ ...s, name: e.target.value }))} />
            <input className="w-full rounded border p-2" placeholder="Trunk ID" value={trunk.trunk_id} onChange={(e) => setTrunk((s) => ({ ...s, trunk_id: e.target.value }))} />
            <input className="w-full rounded border p-2" placeholder="Primary Phone Number" value={trunk.phone_number} onChange={(e) => setTrunk((s) => ({ ...s, phone_number: e.target.value }))} />
            <input className="w-full rounded border p-2" placeholder="SIP Trunk URI (sip:trunk.example.com)" value={trunk.sip_uri} onChange={(e) => setTrunk((s) => ({ ...s, sip_uri: e.target.value }))} />
            <input className="w-full rounded border p-2" placeholder="SIP Username" value={trunk.sip_username} onChange={(e) => setTrunk((s) => ({ ...s, sip_username: e.target.value }))} />
            <input className="w-full rounded border p-2" type="password" placeholder="SIP Password" value={trunk.sip_password} onChange={(e) => setTrunk((s) => ({ ...s, sip_password: e.target.value }))} />
            <input className="w-full rounded border p-2" placeholder="Number Pool (comma separated)" value={trunk.number_pool} onChange={(e) => setTrunk((s) => ({ ...s, number_pool: e.target.value }))} />
          </WizardStep>
        ) : null}

        {step === 4 ? (
          <WizardStep title="Create Your First Agent" actions={<button className="rounded bg-gray-900 px-4 py-2 text-white" onClick={createAgent}>Continue</button>}>
            <input className="w-full rounded border p-2" placeholder="Agent Name" value={agent.name} onChange={(e) => setAgent((s) => ({ ...s, name: e.target.value }))} />
            <select className="w-full rounded border p-2" value={agent.stt_language} onChange={(e) => setAgent((s) => ({ ...s, stt_language: e.target.value }))}>
              {['hi-IN', 'en-IN', 'ta-IN'].map((lang) => <option key={lang}>{lang}</option>)}
            </select>
            <select className="w-full rounded border p-2" value={agent.llm_model} onChange={(e) => setAgent((s) => ({ ...s, llm_model: e.target.value }))}>
              {['gpt-4.1-mini', 'gpt-4o', 'gpt-3.5-turbo'].map((m) => <option key={m}>{m}</option>)}
            </select>
            <input className="w-full rounded border p-2" placeholder="TTS Voice" value={agent.tts_voice} onChange={(e) => setAgent((s) => ({ ...s, tts_voice: e.target.value }))} />
            <textarea className="w-full rounded border p-2" rows={4} placeholder="System Prompt" value={agent.system_prompt} onChange={(e) => setAgent((s) => ({ ...s, system_prompt: e.target.value }))} />
            <textarea className="w-full rounded border p-2" rows={2} placeholder="First Line" value={agent.first_line} onChange={(e) => setAgent((s) => ({ ...s, first_line: e.target.value }))} />
          </WizardStep>
        ) : null}

        {step === 5 ? (
          <WizardStep title="Test Call" subtitle="Dial a test call to verify everything works" actions={<button className="rounded bg-gray-900 px-4 py-2 text-white" onClick={runTestCall}>Dial Test Call</button>}>
            <input className="w-full rounded border p-2" placeholder="Phone (E.164)" value={testPhone} onChange={(e) => setTestPhone(e.target.value)} />
            <p className="text-sm text-gray-600 dark:text-gray-300">{status}</p>
          </WizardStep>
        ) : null}

        {step === 6 ? (
          <WizardStep title="You&apos;re Ready!" subtitle="Setup complete. Start using RapidXAI now.">
            <div className="grid gap-2 sm:grid-cols-2">
              <a href="/campaigns" className="rounded border p-3">Create Campaign</a>
              <a href="/leads" className="rounded border p-3">Import Leads</a>
              <a href="/live-monitor" className="rounded border p-3">View Dashboard</a>
              <a href="/analytics" className="rounded border p-3">View Analytics</a>
            </div>
          </WizardStep>
        ) : null}
      </div>
    </div>
  );
}
