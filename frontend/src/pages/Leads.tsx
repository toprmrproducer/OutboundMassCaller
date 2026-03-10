import { useEffect, useMemo, useState } from "react";
import BulkActionBar, { BulkAction } from "../components/BulkActionBar";
import LeadImporter from "../components/LeadImporter";

type Lead = { id: string; phone: string; name?: string; status?: string; tags?: string[]; temperature?: string };

export default function LeadsPage() {
  const campaignId = localStorage.getItem("campaign_id") || "";
  const businessId = localStorage.getItem("business_id") || "";
  const [rows, setRows] = useState<Lead[]>([]);
  const [selected, setSelected] = useState<string[]>([]);

  const load = async () => {
    if (!campaignId) return;
    const res = await fetch(`/api/campaigns/${campaignId}/leads`);
    const data = await res.json();
    setRows(Array.isArray(data) ? data : data?.data || []);
  };

  useEffect(() => {
    load();
  }, [campaignId]);

  const selectedRows = useMemo(() => rows.filter((r) => selected.includes(r.id)), [rows, selected]);

  const actions: BulkAction[] = [
    {
      label: "Add to DNC",
      onClick: async (ids) => {
        await fetch("/api/leads/bulk-dnc", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ business_id: businessId, lead_ids: ids }),
        });
        setSelected([]);
        load();
      },
    },
    {
      label: "Change Status",
      onClick: async (ids) => {
        const status = prompt("New status:") || "pending";
        await fetch("/api/leads/bulk-status", {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ business_id: businessId, lead_ids: ids, status }),
        });
        setSelected([]);
        load();
      },
    },
    {
      label: "Add Tag",
      onClick: async (ids) => {
        const tag = prompt("Tag:") || "priority";
        await fetch("/api/leads/bulk-tag", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ business_id: businessId, lead_ids: ids, tag }),
        });
        setSelected([]);
        load();
      },
    },
    {
      label: "Delete",
      variant: "destructive",
      onClick: async (ids) => {
        await fetch("/api/leads/bulk", {
          method: "DELETE",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ business_id: businessId, lead_ids: ids }),
        });
        setSelected([]);
        load();
      },
    },
  ];

  return (
    <div className="space-y-4 p-4">
      <h1 className="text-xl font-semibold">Leads</h1>
      {campaignId ? <LeadImporter campaignId={campaignId} businessId={businessId} onDone={load} /> : null}
      <div className="rounded border bg-white">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b text-left">
              <th className="p-2" />
              <th className="p-2">Phone</th>
              <th className="p-2">Name</th>
              <th className="p-2">Status</th>
              <th className="p-2">Temp</th>
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
                      setSelected((prev) =>
                        e.target.checked ? [...prev, r.id] : prev.filter((x) => x !== r.id)
                      );
                    }}
                  />
                </td>
                <td className="p-2">{r.phone}</td>
                <td className="p-2">{r.name || "-"}</td>
                <td className="p-2">{r.status || "-"}</td>
                <td className="p-2">{r.temperature || "warm"}</td>
              </tr>
            ))}
            {rows.length === 0 ? (
              <tr>
                <td className="p-8 text-center text-gray-500" colSpan={5}>No leads yet</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
      <BulkActionBar selectedIds={selectedRows.map((x) => x.id)} actions={actions} onClear={() => setSelected([])} />
    </div>
  );
}
