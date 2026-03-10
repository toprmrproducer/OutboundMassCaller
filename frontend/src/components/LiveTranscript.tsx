import { useEffect, useMemo, useRef, useState } from "react";

type TranscriptRow = {
  role: string;
  content: string;
  turn_number: number;
  created_at?: string;
};

type Props = {
  roomId: string;
};

export default function LiveTranscript({ roomId }: Props) {
  const [rows, setRows] = useState<TranscriptRow[]>([]);
  const [typing, setTyping] = useState(false);
  const listRef = useRef<HTMLDivElement | null>(null);
  const typingTimerRef = useRef<number | undefined>(undefined);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let cancelled = false;

    const loadHistory = async () => {
      try {
        const res = await fetch(`/api/calls/${encodeURIComponent(roomId)}/transcript`);
        const data = await res.json();
        if (!cancelled) {
          setRows(Array.isArray(data) ? data : data?.data || []);
        }
      } catch {
        // noop
      }
    };

    const connect = () => {
      const proto = window.location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${window.location.host}/ws/live`);
      ws.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          if (payload?.event !== "transcript_line") return;
          if (payload?.room_id !== roomId) return;

          const next: TranscriptRow = {
            role: payload.role === "agent" ? "assistant" : payload.role,
            content: payload.content,
            turn_number: payload.turn_number,
            created_at: payload.timestamp,
          };
          setRows((prev) => [...prev, next]);

          if (next.role === "user") {
            setTyping(true);
            if (typingTimerRef.current) window.clearTimeout(typingTimerRef.current);
            typingTimerRef.current = window.setTimeout(() => setTyping(false), 3000);
          }
          if (next.role === "assistant") {
            setTyping(false);
            if (typingTimerRef.current) window.clearTimeout(typingTimerRef.current);
          }
        } catch {
          // noop
        }
      };
    };

    loadHistory();
    connect();

    return () => {
      cancelled = true;
      if (typingTimerRef.current) window.clearTimeout(typingTimerRef.current);
      ws?.close();
    };
  }, [roomId]);

  useEffect(() => {
    const el = listRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [rows, typing]);

  const rendered = useMemo(() => {
    return rows.map((r, idx) => {
      const isAgent = r.role === "assistant" || r.role === "agent";
      return (
        <div key={`${r.turn_number}-${idx}`} className={`flex ${isAgent ? "justify-end" : "justify-start"}`}>
          <div
            className={`max-w-[85%] rounded-xl px-3 py-2 text-sm shadow-sm ${
              isAgent ? "bg-indigo-600 text-white" : "bg-slate-100 text-slate-900"
            }`}
          >
            <div>{r.content}</div>
            <div className={`mt-1 text-[10px] ${isAgent ? "text-indigo-100" : "text-slate-500"}`}>
              #{r.turn_number} · {r.created_at ? new Date(r.created_at).toLocaleTimeString() : "now"}
            </div>
          </div>
        </div>
      );
    });
  }, [rows]);

  return (
    <div className="rounded-xl border bg-white p-3">
      <div className="mb-2 text-sm font-semibold text-slate-700">Live Transcript</div>
      <div ref={listRef} className="h-72 space-y-2 overflow-y-auto rounded border bg-slate-50 p-2">
        {rendered}
        {typing ? (
          <div className="text-xs text-slate-500">Agent is typing...</div>
        ) : null}
      </div>
    </div>
  );
}
