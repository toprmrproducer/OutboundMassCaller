import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Bar, BarChart, Funnel, FunnelChart, Line, LineChart, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

type Option = { id: string; name: string };

export default function Analytics() {
  const [businessId, setBusinessId] = useState("");
  const [campaignId, setCampaignId] = useState("");
  const [fromDate, setFromDate] = useState(new Date().toISOString().slice(0, 10));
  const [toDate, setToDate] = useState(new Date().toISOString().slice(0, 10));
  const [campaigns, setCampaigns] = useState<Option[]>([]);
  const [hourly, setHourly] = useState<any[]>([]);
  const [disposition, setDisposition] = useState<Record<string, number>>({});
  const [funnel, setFunnel] = useState<any>({ total_leads: 0, dialed: 0, connected: 0, interested: 0, booked: 0, conversion_rate_pct: 0 });
  const [cost, setCost] = useState<any>({ by_campaign: [] });
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const loadCampaigns = async () => {
      if (!businessId) return;
      const res = await fetch(`/api/campaigns?business_id=${encodeURIComponent(businessId)}`);
      const data = await res.json();
      const list = (data || []).map((c: any) => ({ id: String(c.id), name: c.name || String(c.id) }));
      setCampaigns(list);
      if (!campaignId && list.length > 0) setCampaignId(list[0].id);
    };
    loadCampaigns();
  }, [businessId]);

  const load = async () => {
    if (!businessId || !campaignId) return;
    setLoading(true);
    const [h, d, f, c] = await Promise.all([
      fetch(`/api/analytics/hourly?campaign_id=${campaignId}&business_id=${businessId}&date=${fromDate}`).then((r) => r.json()),
      fetch(`/api/analytics/disposition?campaign_id=${campaignId}&business_id=${businessId}`).then((r) => r.json()),
      fetch(`/api/analytics/funnel?campaign_id=${campaignId}&business_id=${businessId}`).then((r) => r.json()),
      fetch(`/api/analytics/cost?business_id=${businessId}&from=${fromDate}&to=${toDate}`).then((r) => r.json()),
    ]);
    setHourly(h?.data || []);
    setDisposition(d?.data || {});
    setFunnel(f?.data || {});
    setCost(c?.data || { by_campaign: [] });
    setLoading(false);
  };

  const pieData = useMemo(() => Object.entries(disposition).map(([name, value]) => ({ name, value })), [disposition]);
  const funnelData = useMemo(
    () => [
      { name: "Total", value: funnel.total_leads || 0 },
      { name: "Dialed", value: funnel.dialed || 0 },
      { name: "Connected", value: funnel.connected || 0 },
      { name: "Interested", value: funnel.interested || 0 },
      { name: "Booked", value: funnel.booked || 0 },
    ],
    [funnel]
  );

  const exportCsv = () => {
    const rows = ["metric,value", `total_leads,${funnel.total_leads || 0}`, `booked,${funnel.booked || 0}`, `conversion_rate_pct,${funnel.conversion_rate_pct || 0}`];
    const blob = new Blob([rows.join("\n")], { type: "text/csv;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "analytics.csv";
    a.click();
  };

  return (
    <div className="min-h-screen bg-slate-50 p-4 md:p-6">
      <div className="mx-auto max-w-7xl space-y-4">
        <h1 className="text-2xl font-semibold">Analytics</h1>
        <div className="grid gap-2 md:grid-cols-6">
          <input className="rounded border p-2 md:col-span-2" placeholder="Business ID" value={businessId} onChange={(e) => setBusinessId(e.target.value)} />
          <select className="rounded border p-2" value={campaignId} onChange={(e) => setCampaignId(e.target.value)}>
            {campaigns.map((c) => (
              <option key={c.id} value={c.id}>{c.name}</option>
            ))}
          </select>
          <input className="rounded border p-2" type="date" value={fromDate} onChange={(e) => setFromDate(e.target.value)} />
          <input className="rounded border p-2" type="date" value={toDate} onChange={(e) => setToDate(e.target.value)} />
          <button className="rounded bg-slate-900 px-3 py-2 text-white" onClick={load}>Load</button>
        </div>

        <div className="flex gap-2">
          <button className="rounded border px-3 py-2" onClick={exportCsv}>Export Data CSV</button>
          <button className="rounded border px-3 py-2" onClick={() => window.print()}>Export Charts PNG</button>
        </div>

        {loading ? (
          <div className="grid gap-3 md:grid-cols-2">
            <div className="h-56 animate-pulse rounded border bg-white" />
            <div className="h-56 animate-pulse rounded border bg-white" />
            <div className="h-56 animate-pulse rounded border bg-white" />
            <div className="h-56 animate-pulse rounded border bg-white" />
          </div>
        ) : (
          <div className="grid gap-3 md:grid-cols-2">
            <Card title="Calls per Hour">
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={hourly}>
                  <XAxis dataKey="hour" />
                  <YAxis />
                  <Tooltip />
                  <Line type="monotone" dataKey="calls" stroke="#0f766e" />
                </LineChart>
              </ResponsiveContainer>
            </Card>

            <Card title="Disposition Breakdown">
              <ResponsiveContainer width="100%" height={220}>
                <PieChart>
                  <Tooltip />
                  <Pie data={pieData} dataKey="value" nameKey="name" outerRadius={80} />
                </PieChart>
              </ResponsiveContainer>
            </Card>

            <Card title="Conversion Funnel">
              <ResponsiveContainer width="100%" height={220}>
                <FunnelChart>
                  <Tooltip />
                  <Funnel dataKey="value" data={funnelData} isAnimationActive={false} />
                </FunnelChart>
              </ResponsiveContainer>
            </Card>

            <Card title="Cost per Campaign">
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={cost.by_campaign || []}>
                  <XAxis dataKey="campaign_name" />
                  <YAxis />
                  <Tooltip />
                  <Bar dataKey="total_cost_usd" fill="#0f172a" />
                </BarChart>
              </ResponsiveContainer>
            </Card>
          </div>
        )}
      </div>
    </div>
  );
}

function Card({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded border bg-white p-3 shadow-sm">
      <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-slate-600">{title}</h2>
      {children}
    </div>
  );
}
