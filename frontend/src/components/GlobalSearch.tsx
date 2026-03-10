import { useEffect, useMemo, useState } from "react";

type SearchItem = { type: string; id: string; label: string; subtitle?: string; url: string };

type Props = { businessId: string; onNavigate?: (url: string) => void };

export default function GlobalSearch({ businessId, onNavigate }: Props) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const [results, setResults] = useState<SearchItem[]>([]);

  useEffect(() => {
    const h = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen((v) => !v);
      }
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("keydown", h);
    return () => document.removeEventListener("keydown", h);
  }, []);

  useEffect(() => {
    if (!open || !q.trim()) {
      setResults([]);
      return;
    }
    const t = setTimeout(async () => {
      const res = await fetch(`/api/search?business_id=${encodeURIComponent(businessId)}&q=${encodeURIComponent(q)}&limit=20`);
      const json = await res.json();
      setResults((json?.data?.results || json?.results || []) as SearchItem[]);
      const prev = JSON.parse(localStorage.getItem("recent_searches") || "[]") as string[];
      const next = [q, ...prev.filter((x) => x !== q)].slice(0, 10);
      localStorage.setItem("recent_searches", JSON.stringify(next));
    }, 300);
    return () => clearTimeout(t);
  }, [open, q, businessId]);

  const grouped = useMemo(() => {
    const map: Record<string, SearchItem[]> = {};
    for (const r of results) {
      map[r.type] = map[r.type] || [];
      map[r.type].push(r);
    }
    return map;
  }, [results]);

  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 bg-black/30 p-4" onClick={() => setOpen(false)}>
      <div className="mx-auto max-w-2xl rounded-xl border bg-white p-4 shadow-xl" onClick={(e) => e.stopPropagation()}>
        <input
          autoFocus
          className="w-full rounded border p-3"
          placeholder="Search leads, calls, campaigns, agents..."
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
        <div className="mt-3 max-h-[60vh] overflow-y-auto text-sm">
          {Object.entries(grouped).map(([type, items]) => (
            <div key={type} className="mb-3">
              <div className="mb-1 text-xs uppercase tracking-wide text-gray-500">{type}</div>
              {items.map((item) => (
                <button
                  key={`${item.type}-${item.id}`}
                  className="mb-1 w-full rounded border p-2 text-left hover:bg-gray-50"
                  onClick={() => {
                    setOpen(false);
                    if (onNavigate) onNavigate(item.url);
                    else window.location.href = item.url;
                  }}
                >
                  <div className="font-medium">{item.label}</div>
                  <div className="text-xs text-gray-500">{item.subtitle}</div>
                </button>
              ))}
            </div>
          ))}
          {results.length === 0 ? <div className="py-4 text-center text-gray-500">No results</div> : null}
        </div>
      </div>
    </div>
  );
}
