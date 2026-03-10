import { useMemo, useState } from "react";

type BookingEvent = {
  id: string;
  title: string;
  start: string;
  end?: string;
  status: string;
  call_id?: string;
  campaign_name?: string;
  notes?: string;
  color?: string;
};

export default function BookingCalendar() {
  const today = new Date().toISOString().slice(0, 10);
  const [businessId, setBusinessId] = useState("");
  const [fromDate, setFromDate] = useState(today);
  const [toDate, setToDate] = useState(today);
  const [events, setEvents] = useState<BookingEvent[]>([]);
  const [summary, setSummary] = useState<any>({ today: 0, this_week: 0, this_month: 0 });

  const grouped = useMemo(() => {
    const map: Record<string, BookingEvent[]> = {};
    for (const e of events) {
      const d = (e.start || "").slice(0, 10);
      if (!map[d]) map[d] = [];
      map[d].push(e);
    }
    return map;
  }, [events]);

  const load = async () => {
    if (!businessId) return;
    const [calendarRes, summaryRes] = await Promise.all([
      fetch(`/api/bookings/calendar?business_id=${encodeURIComponent(businessId)}&from=${fromDate}&to=${toDate}`).then((r) => r.json()),
      fetch(`/api/bookings/summary?business_id=${encodeURIComponent(businessId)}`).then((r) => r.json()),
    ]);
    setEvents(calendarRes?.data || []);
    setSummary(summaryRes?.data || {});
  };

  const patchBooking = async (id: string, payload: any) => {
    await fetch(`/api/bookings/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    await load();
  };

  return (
    <div className="min-h-screen bg-slate-50 p-4 md:p-6">
      <div className="mx-auto max-w-7xl space-y-4">
        <h1 className="text-2xl font-semibold">Bookings Calendar</h1>

        <div className="grid gap-3 md:grid-cols-3">
          <Stat label="Today's Bookings" value={summary.today || 0} />
          <Stat label="This Week" value={summary.this_week || 0} />
          <Stat label="This Month" value={summary.this_month || 0} />
        </div>

        <div className="flex flex-wrap gap-2">
          <input className="rounded border p-2" placeholder="Business ID" value={businessId} onChange={(e) => setBusinessId(e.target.value)} />
          <input className="rounded border p-2" type="date" value={fromDate} onChange={(e) => setFromDate(e.target.value)} />
          <input className="rounded border p-2" type="date" value={toDate} onChange={(e) => setToDate(e.target.value)} />
          <button className="rounded bg-slate-900 px-3 py-2 text-white" onClick={load}>Load</button>
        </div>

        <div className="rounded border bg-white p-3">
          {Object.keys(grouped).length === 0 ? (
            <div className="py-12 text-center text-slate-500">No bookings in selected range</div>
          ) : (
            Object.entries(grouped).map(([day, list]) => (
              <div key={day} className="mb-4">
                <div className="mb-2 text-sm font-semibold text-slate-700">{day}</div>
                <div className="space-y-2">
                  {list.map((event) => (
                    <div key={event.id} className="rounded border p-3">
                      <div className="flex items-center justify-between">
                        <div>
                          <div className="font-medium">{event.title}</div>
                          <div className="text-xs text-slate-500">{new Date(event.start).toLocaleString()} · {event.campaign_name || ""}</div>
                        </div>
                        <span className={`rounded px-2 py-1 text-xs ${event.status === "cancelled" ? "bg-red-100 text-red-700" : "bg-emerald-100 text-emerald-700"}`}>
                          {event.status}
                        </span>
                      </div>
                      <div className="mt-2 flex gap-2">
                        <button className="rounded border px-2 py-1" onClick={() => patchBooking(event.id, { status: "confirmed" })}>Reschedule</button>
                        <button className="rounded border px-2 py-1" onClick={() => patchBooking(event.id, { status: "cancelled" })}>Cancel</button>
                        <button className="rounded border px-2 py-1">Call Again</button>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded border bg-white p-4 shadow-sm">
      <div className="text-xs uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-slate-900">{value}</div>
    </div>
  );
}
