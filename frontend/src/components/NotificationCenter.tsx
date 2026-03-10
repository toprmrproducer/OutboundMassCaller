import { useState } from "react";
import useNotifications from "../hooks/useNotifications";

type Props = { businessId: string };

export default function NotificationCenter({ businessId }: Props) {
  const [open, setOpen] = useState(false);
  const { items, unreadCount, loading, fetchItems, fetchUnread } = useNotifications(businessId);

  const markAllRead = async () => {
    await fetch("/api/notifications/mark-all-read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ business_id: businessId }),
    });
    await fetchItems();
    await fetchUnread();
  };

  const markRead = async (id: string) => {
    await fetch(`/api/notifications/${id}/read?business_id=${encodeURIComponent(businessId)}`, { method: "POST" });
    await fetchItems();
    await fetchUnread();
  };

  return (
    <div className="relative">
      <button className="relative rounded border px-3 py-2" onClick={() => setOpen((v) => !v)}>
        Notifications
        {unreadCount > 0 ? (
          <span className="absolute -right-2 -top-2 rounded-full bg-red-600 px-1.5 text-xs text-white">{unreadCount}</span>
        ) : null}
      </button>
      {open ? (
        <div className="absolute right-0 z-50 mt-2 w-96 max-h-[70vh] overflow-y-auto rounded border bg-white p-3 shadow-lg">
          <div className="mb-2 flex items-center justify-between">
            <div className="font-semibold">Notifications</div>
            <button className="text-xs text-blue-600" onClick={markAllRead}>Mark all read</button>
          </div>
          {loading ? <div className="text-sm text-gray-500">Loading...</div> : null}
          {items.length === 0 ? <div className="text-sm text-gray-500">No notifications</div> : null}
          <div className="space-y-2">
            {items.map((n) => (
              <button
                key={n.id}
                className={`w-full rounded border p-2 text-left ${n.is_read ? "" : "border-l-4 border-l-blue-500"}`}
                onClick={() => {
                  markRead(n.id);
                  if (n.resource_type && n.resource_id) {
                    window.location.href = `/${n.resource_type}s/${n.resource_id}`;
                  }
                }}
              >
                <div className="text-xs uppercase text-gray-500">{n.type}</div>
                <div className="font-medium">{n.title}</div>
                <div className="text-sm text-gray-600">{n.body}</div>
                <div className="text-xs text-gray-400">{new Date(n.created_at).toLocaleString()}</div>
              </button>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
