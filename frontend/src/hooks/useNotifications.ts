import { useEffect, useState } from "react";

type NotificationItem = {
  id: string;
  type: string;
  title: string;
  body: string;
  is_read: boolean;
  created_at: string;
  resource_type?: string;
  resource_id?: string;
};

export default function useNotifications(businessId: string) {
  const [items, setItems] = useState<NotificationItem[]>([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [loading, setLoading] = useState(false);

  const fetchItems = async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/notifications?business_id=${encodeURIComponent(businessId)}&limit=50`);
      const json = await res.json();
      setItems((json?.data || []) as NotificationItem[]);
    } finally {
      setLoading(false);
    }
  };

  const fetchUnread = async () => {
    const res = await fetch(`/api/notifications/unread-count?business_id=${encodeURIComponent(businessId)}`);
    const json = await res.json();
    setUnreadCount(Number(json?.data?.count || 0));
  };

  useEffect(() => {
    if (!businessId) return;
    fetchItems();
    fetchUnread();
    const timer = setInterval(fetchUnread, 30000);
    return () => clearInterval(timer);
  }, [businessId]);

  return { items, unreadCount, loading, fetchItems, fetchUnread };
}
