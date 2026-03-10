import { useEffect, useState } from "react";
import BulkActionBar, { BulkAction } from "../components/BulkActionBar";

type Campaign = { id: string; name: string; status: string };

export default function CampaignsPage() {
  const businessId = localStorage.getItem("business_id") || "";
  const [rows, setRows] = useState<Campaign[]>([]);
  const [selected, setSelected] = useState<string[]>([]);

  const load = async () => {
    if (!businessId) return;
    const res = await fetch(`/api/campaigns?business_id=${encodeURIComponent(businessId)}`);
    const data = await res.json();
    setRows(Array.isArray(data) ? data : data?.data || []);
  };

  useEffect(() => {
    load();
  }, [businessId]);

  const actions: BulkAction[] = [
    {
      label: "Pause Selected",
      onClick: async (ids) => {
        await fetch("/api/campaigns/bulk-pause", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ business_id: businessId, campaign_ids: ids }),
        });
        setSelected([]);
        load();
      },
    },
    {
      label: "Resume Selected",
      onClick: async (ids) => {
        await fetch("/api/campaigns/bulk-resume", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ business_id: businessId, campaign_ids: ids }),
        });
        setSelected([]);
        load();
      },
    },
  ];

  return (
    <div className="space-y-4 p-4">
      <h1 className="text-xl font-semibold">Campaigns</h1>
      <div className="rounded border bg-white">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b text-left">
              <th className="p-2" />
              <th className="p-2">Name</th>
              <th className="p-2">Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id} className="border-b">
                <td className="p-2">
                  <input
                    type="checkbox"
                    checked={selected.includes(r.id)}
                    onChange={(e) => {
                      setSelected((prev) => (e.target.checked ? [...prev, r.id] : prev.filter((x) => x !== r.id)));
                    }}
                  />
                </td>
                <td className="p-2">{r.name}</td>
                <td className="p-2">{r.status}</td>
              </tr>
            ))}
            {rows.length === 0 ? (
              <tr>
                <td className="p-8 text-center text-gray-500" colSpan={3}>No campaigns yet</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
      <BulkActionBar selectedIds={selected} actions={actions} onClear={() => setSelected([])} />
    </div>
  );
}
