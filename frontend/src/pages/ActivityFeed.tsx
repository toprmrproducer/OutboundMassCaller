import { useEffect, useMemo, useState } from "react";

type Activity = {
  type: string;
  id: string;
  label: string;
  action: string;
  created_at: string;
  ref_id?: string;
};

const FILTERS = ["all", "call", "booking", "campaign", "notification"] as const;

export default function ActivityFeed() {
  const businessId = localStorage.getItem("business_id") || "";
  const [items, setItems] = useState<Activity[]>([]);
  const [filter, setFilter] = useState<(typeof FILTERS)[number]>("all");
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);

  const load = async (append = false) => {
    if (!businessId) return;
    setLoading(true);
    const res = await fetch(`/api/activity-feed?business_id=${encodeURIComponent(businessId)}&limit=50&offset=${append ? offset : 0}`);
    const json = await res.json();
    const rows = (json?.data || []) as Activity[];
    setItems((prev) => (append ? [...prev, ...rows] : rows));
    setLoading(false);
  };

  useEffect(() => {
    load(false);
    const t = setInterval(() => load(false), 30000);
    return () => clearInterval(t);
  }, [businessId]);

  const filtered = useMemo(() => {
    if (filter === "all") return items;
    return items.filter((i) => i.type === filter);
  }, [items, filter]);

  return (
    <div className="min-h-screen bg-gray-50 p-4 dark:bg-gray-950">
      <div className="mx-auto max-w-5xl rounded-xl border bg-white p-4 dark:border-gray-700 dark:bg-gray-900">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <h1 className="text-xl font-semibold">Activity Feed</h1>
          <div className="flex gap-2">
            {FILTERS.map((f) => (
              <button
                key={f}
                onClick={() => setFilter(f)}
                className={`rounded px-3 py-1 text-sm ${filter === f ? "bg-gray-900 text-white" : "border"}`}
              >
                {f}
              </button>
            ))}
          </div>
        </div>

        <div className="space-y-2">
          {filtered.map((item) => (
            <button
              key={`${item.type}-${item.id}-${item.created_at}`}
              className="w-full rounded border p-3 text-left hover:bg-gray-50 dark:hover:bg-gray-800"
              onClick={() => {
                const base = item.type === "call" ? "/calls" : item.type === "booking" ? "/bookings" : item.type === "campaign" ? "/campaigns" : "/notifications";
                window.location.href = `${base}/${item.ref_id || item.id}`;
              }}
            >
              <div className="text-xs uppercase text-gray-500">{item.type}</div>
              <div className="font-medium">{item.label}</div>
              <div className="text-sm text-gray-600">{item.action}</div>
              <div className="text-xs text-gray-400">{new Date(item.created_at).toLocaleString()}</div>
            </button>
          ))}
          {filtered.length === 0 && !loading ? <div className="py-10 text-center text-gray-500">No activity yet</div> : null}
          {loading ? <div className="text-sm text-gray-500">Loading…</div> : null}
        </div>

        <div className="mt-3 text-center">
          <button
            className="rounded border px-4 py-2 text-sm"
            onClick={async () => {
              const next = offset + 50;
              setOffset(next);
              await load(true);
            }}
          >
            Load more
          </button>
        </div>
      </div>
    </div>
  );
}
