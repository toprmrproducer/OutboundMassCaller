import { useEffect } from "react";

export const SHORTCUTS = [
  { keys: ["cmd+k", "ctrl+k"], action: "open_search", label: "Open Search" },
  { keys: ["cmd+n", "ctrl+n"], action: "new_campaign", label: "New Campaign" },
  { keys: ["cmd+shift+l", "ctrl+shift+l"], action: "go_leads", label: "Go to Leads" },
  { keys: ["cmd+shift+c", "ctrl+shift+c"], action: "go_campaigns", label: "Go to Campaigns" },
  { keys: ["cmd+shift+a", "ctrl+shift+a"], action: "go_analytics", label: "Go to Analytics" },
  { keys: ["cmd+shift+d", "ctrl+shift+d"], action: "go_dashboard", label: "Go to Dashboard" },
  { keys: ["?"], action: "show_shortcuts", label: "Show Keyboard Shortcuts" },
  { keys: ["esc"], action: "close_modal", label: "Close Modal / Dismiss" },
];

type Handlers = {
  openSearch?: () => void;
  showShortcuts?: () => void;
  closeModal?: () => void;
};

export function useKeyboardShortcuts(navigate: (url: string) => void, handlers: Handlers = {}) {
  useEffect(() => {
    const isMac = navigator.platform.toLowerCase().includes("mac");

    const onKey = (e: KeyboardEvent) => {
      const key = e.key.toLowerCase();
      const mod = isMac ? e.metaKey : e.ctrlKey;

      if (mod && key === "k") {
        e.preventDefault();
        handlers.openSearch?.();
        return;
      }
      if (mod && key === "n") {
        e.preventDefault();
        navigate("/campaigns/new");
        return;
      }
      if (mod && e.shiftKey && key === "l") {
        e.preventDefault();
        navigate("/leads");
        return;
      }
      if (mod && e.shiftKey && key === "c") {
        e.preventDefault();
        navigate("/campaigns");
        return;
      }
      if (mod && e.shiftKey && key === "a") {
        e.preventDefault();
        navigate("/analytics");
        return;
      }
      if (mod && e.shiftKey && key === "d") {
        e.preventDefault();
        navigate("/live-monitor");
        return;
      }
      if (e.key === "?") {
        e.preventDefault();
        handlers.showShortcuts?.();
        return;
      }
      if (e.key === "Escape") {
        handlers.closeModal?.();
      }
    };

    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [navigate, handlers]);
}
