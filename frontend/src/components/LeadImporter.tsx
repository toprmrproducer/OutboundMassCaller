import { useMemo, useState } from "react";

type PreviewRow = Record<string, string>;

type Props = {
  campaignId: string;
  businessId: string;
  onDone?: () => void;
};

export default function LeadImporter({ campaignId, businessId, onDone }: Props) {
  const [file, setFile] = useState<File | null>(null);
  const [preview, setPreview] = useState<PreviewRow[]>([]);
  const [status, setStatus] = useState<string>("");
  const [progress, setProgress] = useState(0);
  const [result, setResult] = useState<any>(null);

  const columns = useMemo(() => {
    if (preview.length === 0) return [];
    return Object.keys(preview[0]);
  }, [preview]);

  const readPreview = async (f: File) => {
    const text = await f.text();
    const lines = text.split(/\r?\n/).filter(Boolean);
    const headers = (lines[0] || "").split(",").map((h) => h.trim());
    const rows = lines.slice(1, 6).map((line) => {
      const vals = line.split(",");
      const row: PreviewRow = {};
      headers.forEach((h, i) => {
        row[h] = (vals[i] || "").trim();
      });
      return row;
    });
    setPreview(rows);
  };

  const onFile = async (f?: File) => {
    if (!f) return;
    setFile(f);
    await readPreview(f);
  };

  const importNow = async () => {
    if (!file) return;
    setStatus("Uploading...");
    setProgress(20);
    const fd = new FormData();
    fd.append("file", file);
    const res = await fetch(`/api/campaigns/${campaignId}/leads/import?business_id=${encodeURIComponent(businessId)}`, {
      method: "POST",
      body: fd,
    });
    setProgress(80);
    const body = await res.json();
    setResult(body?.data || body);
    setProgress(100);
    setStatus("Completed");
    if (onDone) onDone();
  };

  const downloadErrors = () => {
    const errors = (result?.errors || []) as Array<{ row: number; phone: string; reason: string }>;
    const lines = ["row,phone,reason", ...errors.map((e) => `${e.row},${e.phone},${e.reason}`)];
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "lead_import_errors.csv";
    a.click();
  };

  return (
    <div className="space-y-3 rounded border bg-white p-4">
      <h3 className="text-lg font-semibold">Lead Importer</h3>
      <label className="block rounded border-2 border-dashed p-6 text-center text-sm text-slate-600">
        Drag and drop CSV or click to upload
        <input className="hidden" type="file" accept=".csv" onChange={(e) => onFile(e.target.files?.[0])} />
      </label>

      {columns.length > 0 && (
        <div className="rounded border p-3">
          <div className="mb-2 text-sm font-medium">Column Mapping</div>
          <div className="grid gap-2 md:grid-cols-2">
            <div>
              <label className="text-xs text-slate-500">phone (required)</label>
              <select className="w-full rounded border p-2">
                {columns.map((c) => (
                  <option key={c}>{c}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="text-xs text-slate-500">name</label>
              <select className="w-full rounded border p-2">
                {columns.map((c) => (
                  <option key={c}>{c}</option>
                ))}
              </select>
            </div>
          </div>
        </div>
      )}

      {preview.length > 0 && (
        <div className="rounded border p-3">
          <div className="mb-2 text-sm font-medium">Preview (first 5 rows)</div>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr>
                  {Object.keys(preview[0]).map((k) => (
                    <th key={k} className="border-b px-2 py-1 text-left">{k}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {preview.map((r, i) => (
                  <tr key={i}>
                    {Object.keys(r).map((k) => (
                      <td key={k} className="border-b px-2 py-1">{r[k]}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <button className="rounded bg-slate-900 px-3 py-2 text-white" onClick={importNow} disabled={!file}>
        Import
      </button>

      <div className="h-2 rounded bg-slate-200">
        <div className="h-2 rounded bg-emerald-600" style={{ width: `${progress}%` }} />
      </div>
      <div className="text-sm text-slate-600">{status}</div>

      {result && (
        <div className="rounded border p-3 text-sm">
          <div>Imported: {result.imported}</div>
          <div>Duplicates skipped: {result.duplicates_skipped}</div>
          <div>DNC skipped: {result.dnc_skipped}</div>
          <div>Invalid skipped: {result.invalid_skipped}</div>
          <button className="mt-2 rounded border px-2 py-1" onClick={downloadErrors}>
            Download Error Report CSV
          </button>
        </div>
      )}
    </div>
  );
}
