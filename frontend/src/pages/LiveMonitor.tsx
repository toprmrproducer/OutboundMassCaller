import { useEffect, useMemo, useRef, useState } from "react";
import { Line, LineChart, ResponsiveContainer, Tooltip } from "recharts";

type ActiveCall = {
  room_id: string;
  phone: string;
  campaign_id?: string;
  agent_id?: string;
  started_at?: string;
  status?: string;
};

type LivePayload = {
  active_calls: ActiveCall[];
  calls_per_minute_now: number;
  campaign_statuses: Record<string, string>;
  queue_depth: number;
  booked_today?: number;
};

const MAX_POINTS = 150;

export default function LiveMonitor() {
  const [connected, setConnected] = useState(false);
  const [payload, setPayload] = useState<LivePayload>({
    active_calls: [],
    calls_per_minute_now: 0,
    campaign_statuses: {},
    queue_depth: 0,
    booked_today: 0,
  });
  const [series, setSeries] = useState<Array<{ t: string; v: number }>>([]);
  const retryRef = useRef(0);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let timer: number | undefined;

    const connect = () => {
      const proto = window.location.protocol === "https:" ? "wss" : "ws";
      const ws = new WebSocket(`${proto}://${window.location.host}/ws/live`);
      wsRef.current = ws;

      ws.onopen = () => {
        retryRef.current = 0;
        setConnected(true);
      };

      ws.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data) as LivePayload;
          setPayload(data);
          setSeries((prev) => {
            const next = [...prev, { t: new Date().toLocaleTimeString(), v: data.calls_per_minute_now ?? 0 }];
            return next.slice(-MAX_POINTS);
          });
        } catch {
          // ignore malformed frames
        }
      };

      ws.onclose = () => {
        setConnected(false);
        const delay = Math.min(30000, 1000 * Math.pow(2, retryRef.current));
        retryRef.current += 1;
        timer = window.setTimeout(connect, delay);
      };

      ws.onerror = () => {
        ws.close();
      };
    };

    connect();
    return () => {
      if (timer) window.clearTimeout(timer);
      wsRef.current?.close();
    };
  }, []);

  const now = Date.now();
  const rows = useMemo(() => {
    return payload.active_calls.map((c) => {
      const started = c.started_at ? new Date(c.started_at).getTime() : now;
      const sec = Math.max(0, Math.floor((now - started) / 1000));
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");
      return { ...c, duration: `${mm}:${ss}` };
    });
  }, [payload.active_calls, now]);

  return (
    <div className="min-h-screen bg-slate-50 p-4 md:p-6">
      <div className="mx-auto max-w-7xl space-y-4">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-semibold text-slate-900">Live Call Monitor</h1>
          <div className="flex items-center gap-2 text-sm">
            <span
              className={`h-3 w-3 rounded-full ${
                connected ? "animate-pulse bg-emerald-500" : "bg-red-500"
              }`}
            />
            <span className="text-slate-700">{connected ? "Connected" : "Disconnected"}</span>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
          <Stat title="Active Calls" value={String(payload.active_calls.length)} />
          <Stat title="Calls/min" value={String(payload.calls_per_minute_now)} />
          <Stat title="Queue Depth" value={String(payload.queue_depth)} />
          <Stat title="Booked Today" value={String(payload.booked_today ?? 0)} />
        </div>

        <div className="rounded-xl border bg-white p-4 shadow-sm">
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-600">Active Calls</h2>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b text-left text-slate-600">
                  <th className="py-2 pr-3">Phone</th>
                  <th className="py-2 pr-3">Campaign</th>
                  <th className="py-2 pr-3">Agent</th>
                  <th className="py-2 pr-3">Duration</th>
                  <th className="py-2 pr-3">Status</th>
                </tr>
              </thead>
              <tbody>
                {rows.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="py-6 text-center text-slate-500">
                      No active calls
                    </td>
                  </tr>
                ) : (
                  rows.map((r) => (
                    <tr key={r.room_id} className="border-b last:border-b-0">
                      <td className="py-2 pr-3 font-medium">{r.phone}</td>
                      <td className="py-2 pr-3 text-slate-600">{r.campaign_id || "-"}</td>
                      <td className="py-2 pr-3 text-slate-600">{r.agent_id || "-"}</td>
                      <td className="py-2 pr-3 tabular-nums">{r.duration}</td>
                      <td className="py-2 pr-3">
                        <span className="rounded bg-slate-100 px-2 py-1 text-xs">{r.status || "active"}</span>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className="rounded-xl border bg-white p-4 shadow-sm">
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-600">
            Calls per Minute (Last 5 Minutes)
          </h2>
          <div className="h-40 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={series.slice(-150)}>
                <Tooltip />
                <Line dataKey="v" stroke="#0f766e" strokeWidth={2} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}

function Stat({ title, value }: { title: string; value: string }) {
  return (
    <div className="rounded-xl border bg-white p-4 shadow-sm">
      <div className="text-xs uppercase tracking-wide text-slate-500">{title}</div>
      <div className="mt-2 text-2xl font-semibold text-slate-900">{value}</div>
    </div>
  );
}
