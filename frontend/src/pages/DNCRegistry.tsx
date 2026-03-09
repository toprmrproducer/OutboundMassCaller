import { useMemo, useState } from "react";

type DncRow = {
  id: string;
  phone: string;
  reason?: string;
  added_by?: string;
  created_at?: string;
};

export default function DNCRegistry() {
  const [businessId, setBusinessId] = useState("");
  const [search, setSearch] = useState("");
  const [phone, setPhone] = useState("");
  const [reason, setReason] = useState("manual_add");
  const [bulk, setBulk] = useState("");
  const [rows, setRows] = useState<DncRow[]>([]);
  const [loading, setLoading] = useState(false);

  const filtered = useMemo(() => {
    const q = search.trim();
    if (!q) return rows;
    return rows.filter((r) => r.phone.includes(q));
  }, [rows, search]);

  const load = async () => {
    if (!businessId) return;
    setLoading(true);
    const res = await fetch(`/api/dnc?business_id=${encodeURIComponent(businessId)}`);
    const data = await res.json();
    setRows(data?.data || []);
    setLoading(false);
  };

  const addOne = async () => {
    const res = await fetch("/api/dnc", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ business_id: businessId, phone, reason, added_by: "ui" }),
    });
    if (res.ok) {
      setPhone("");
      await load();
    }
  };

  const removeOne = async (p: string) => {
    const res = await fetch(`/api/dnc/${encodeURIComponent(p)}?business_id=${encodeURIComponent(businessId)}`, {
      method: "DELETE",
    });
    if (res.ok) await load();
  };

  const addBulk = async () => {
    const phones = bulk
      .split(/[\n,\s]+/)
      .map((x) => x.trim())
      .filter(Boolean);
    await fetch("/api/dnc/bulk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ business_id: businessId, phones, reason: "bulk_upload" }),
    });
    await load();
  };

  const onFile = async (file?: File) => {
    if (!file) return;
    const text = await file.text();
    setBulk(text);
  };

  const exportCsv = () => {
    const lines = ["phone,reason,added_by,created_at", ...filtered.map((r) => `${r.phone},${r.reason || ""},${r.added_by || ""},${r.created_at || ""}`)];
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "dnc_registry.csv";
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="min-h-screen bg-slate-50 p-4 md:p-6">
      <div className="mx-auto max-w-6xl space-y-4">
        <h1 className="text-2xl font-semibold">DNC Registry</h1>

        <div className="grid gap-2 md:grid-cols-5">
          <input className="rounded border p-2 md:col-span-2" placeholder="Business ID" value={businessId} onChange={(e) => setBusinessId(e.target.value)} />
          <input className="rounded border p-2" placeholder="Phone" value={phone} onChange={(e) => setPhone(e.target.value)} />
          <input className="rounded border p-2" placeholder="Reason" value={reason} onChange={(e) => setReason(e.target.value)} />
          <button className="rounded bg-slate-900 px-3 py-2 text-white" onClick={addOne}>Add</button>
        </div>

        <div className="rounded border bg-white p-3">
          <div className="mb-2 flex flex-wrap gap-2">
            <button className="rounded border px-3 py-2" onClick={load}>Load</button>
            <button className="rounded border px-3 py-2" onClick={exportCsv}>Export CSV</button>
            <input className="rounded border p-2" placeholder="Search phone" value={search} onChange={(e) => setSearch(e.target.value)} />
          </div>

          <textarea className="h-28 w-full rounded border p-2" placeholder="Paste CSV/phones for bulk add" value={bulk} onChange={(e) => setBulk(e.target.value)} />
          <div className="mt-2 flex gap-2">
            <input type="file" accept=".csv,.txt" onChange={(e) => onFile(e.target.files?.[0])} />
            <button className="rounded bg-emerald-700 px-3 py-2 text-white" onClick={addBulk}>Bulk Add</button>
          </div>
        </div>

        <div className="rounded border bg-white p-3">
          {loading ? (
            <div className="h-20 animate-pulse rounded bg-slate-100" />
          ) : (
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b text-left">
                  <th className="py-2 pr-3">Phone</th>
                  <th className="py-2 pr-3">Reason</th>
                  <th className="py-2 pr-3">Added By</th>
                  <th className="py-2 pr-3">Created</th>
                  <th className="py-2 pr-3">Action</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((r) => (
                  <tr key={r.id} className="border-b last:border-b-0">
                    <td className="py-2 pr-3">{r.phone}</td>
                    <td className="py-2 pr-3">{r.reason}</td>
                    <td className="py-2 pr-3">{r.added_by}</td>
                    <td className="py-2 pr-3">{r.created_at}</td>
                    <td className="py-2 pr-3">
                      <button className="rounded bg-red-600 px-2 py-1 text-white" onClick={() => removeOne(r.phone)}>
                        Remove
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
