import { useEffect, useState } from "react";

type SipConfig = {
  business_id: string;
  sip_uri: string;
  username: string;
  password: string;
  caller_id: string;
};

export default function Settings() {
  const [businessId, setBusinessId] = useState(localStorage.getItem("business_id") || "");
  const [form, setForm] = useState<SipConfig>({
    business_id: localStorage.getItem("business_id") || "",
    sip_uri: "",
    username: "",
    password: "",
    caller_id: "",
  });
  const [status, setStatus] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const load = async () => {
      if (!businessId) return;
      setLoading(true);
      try {
        const res = await fetch(`/api/config/sip?business_id=${encodeURIComponent(businessId)}`);
        const json = await res.json();
        const cfg = json?.data || {};
        setForm((prev) => ({
          ...prev,
          business_id: businessId,
          sip_uri: cfg.sip_uri || "",
          username: cfg.username || "",
          password: cfg.password || "",
          caller_id: cfg.caller_id || "",
        }));
      } catch (_e) {
        setStatus("Failed to load SIP configuration");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [businessId]);

  const save = async () => {
    if (!businessId) {
      setStatus("business_id is required");
      return;
    }
    setLoading(true);
    setStatus("");
    try {
      const res = await fetch("/api/config/sip", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...form, business_id: businessId }),
      });
      const json = await res.json();
      if (!res.ok || json?.error) {
        setStatus(json?.error || "Failed to save SIP configuration");
        return;
      }
      setStatus("SIP configuration saved");
    } catch (_e) {
      setStatus("Failed to save SIP configuration");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-semibold">Settings</h1>
      <section className="rounded border bg-white p-4 dark:border-gray-700 dark:bg-gray-800">
        <h2 className="mb-3 text-lg font-medium">SIP Trunk Configuration</h2>
        <div className="grid gap-3 md:grid-cols-2">
          <input
            className="rounded border p-2 dark:border-gray-700 dark:bg-gray-900"
            placeholder="Business ID"
            value={businessId}
            onChange={(e) => {
              const value = e.target.value;
              setBusinessId(value);
              setForm((s) => ({ ...s, business_id: value }));
            }}
          />
          <input
            className="rounded border p-2 dark:border-gray-700 dark:bg-gray-900"
            placeholder="Outbound Caller ID (e.g. +9180...)"
            value={form.caller_id}
            onChange={(e) => setForm((s) => ({ ...s, caller_id: e.target.value }))}
          />
          <input
            className="rounded border p-2 dark:border-gray-700 dark:bg-gray-900 md:col-span-2"
            placeholder="SIP Trunk URI (e.g. sip:trunk.example.com)"
            value={form.sip_uri}
            onChange={(e) => setForm((s) => ({ ...s, sip_uri: e.target.value }))}
          />
          <input
            className="rounded border p-2 dark:border-gray-700 dark:bg-gray-900"
            placeholder="SIP Username"
            value={form.username}
            onChange={(e) => setForm((s) => ({ ...s, username: e.target.value }))}
          />
          <input
            className="rounded border p-2 dark:border-gray-700 dark:bg-gray-900"
            type="password"
            placeholder="SIP Password"
            value={form.password}
            onChange={(e) => setForm((s) => ({ ...s, password: e.target.value }))}
          />
        </div>
        <div className="mt-4 flex items-center gap-3">
          <button className="rounded bg-gray-900 px-4 py-2 text-white disabled:opacity-50" disabled={loading} onClick={save}>
            {loading ? "Saving..." : "Save SIP Config"}
          </button>
          {status ? <p className="text-sm text-gray-600 dark:text-gray-300">{status}</p> : null}
        </div>
      </section>
    </div>
  );
}
