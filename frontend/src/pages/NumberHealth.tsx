import { useState } from "react";

type Row = {
  phone_number: string;
  calls_today?: number;
  calls_this_week?: number;
  spam_score?: number;
  is_paused?: boolean;
  last_checked_at?: string;
  pause_reason?: string;
};

export default function NumberHealth() {
  const [trunkId, setTrunkId] = useState("");
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(false);

  const load = async () => {
    if (!trunkId) return;
    setLoading(true);
    const res = await fetch(`/api/sip-trunks/${trunkId}/number-health`);
    const body = await res.json();
    setRows(body?.data || []);
    setLoading(false);
  };

  const checkAll = async () => {
    if (!trunkId) return;
    await fetch(`/api/sip-trunks/${trunkId}/number-health/check-all`, { method: "POST" });
    await load();
  };

  const togglePause = async (row: Row) => {
    const endpoint = row.is_paused ? "resume" : "pause";
    await fetch(`/api/sip-trunks/${trunkId}/number-health/${encodeURIComponent(row.phone_number)}/${endpoint}`, { method: "POST" });
    await load();
  };

  const scoreColor = (score: number) => {
    if (score <= 30) return "text-emerald-600";
    if (score <= 60) return "text-amber-600";
    return "text-red-600";
  };

  return (
    <div className="min-h-screen bg-slate-50 p-4 md:p-6">
      <div className="mx-auto max-w-6xl space-y-4">
        <h1 className="text-2xl font-semibold">Number Health</h1>

        <div className="flex flex-wrap gap-2">
          <input className="rounded border p-2" placeholder="SIP Trunk ID" value={trunkId} onChange={(e) => setTrunkId(e.target.value)} />
          <button className="rounded border px-3 py-2" onClick={load}>Load</button>
          <button className="rounded bg-indigo-600 px-3 py-2 text-white" onClick={checkAll}>Check All Now</button>
        </div>

        <div className="rounded border bg-white p-3">
          {loading ? <div className="h-16 animate-pulse rounded bg-slate-100" /> : (
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b text-left">
                  <th className="py-2 pr-3">Number</th>
                  <th className="py-2 pr-3">Calls Today</th>
                  <th className="py-2 pr-3">Calls This Week</th>
                  <th className="py-2 pr-3">Spam Score</th>
                  <th className="py-2 pr-3">Status</th>
                  <th className="py-2 pr-3">Last Checked</th>
                  <th className="py-2 pr-3">Actions</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.phone_number} className="border-b last:border-b-0">
                    <td className="py-2 pr-3">{r.phone_number}</td>
                    <td className="py-2 pr-3">{r.calls_today || 0}</td>
                    <td className="py-2 pr-3">{r.calls_this_week || 0}</td>
                    <td className={`py-2 pr-3 font-semibold ${scoreColor(r.spam_score || 0)}`}>{r.spam_score || 0}</td>
                    <td className="py-2 pr-3">{r.is_paused ? "Paused" : "Active"}</td>
                    <td className="py-2 pr-3">{r.last_checked_at || "-"}</td>
                    <td className="py-2 pr-3">
                      <button className="rounded border px-2 py-1" onClick={() => togglePause(r)}>
                        {r.is_paused ? "Resume" : "Pause"}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
