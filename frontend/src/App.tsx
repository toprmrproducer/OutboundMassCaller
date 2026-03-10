import { useState } from "react";
import GlobalSearch from "./components/GlobalSearch";
import InstallPrompt from "./components/InstallPrompt";
import NotificationCenter from "./components/NotificationCenter";
import ShortcutsHelp from "./components/ShortcutsHelp";
import { useKeyboardShortcuts } from "./hooks/useKeyboardShortcuts";
import { useTheme } from "./hooks/useTheme";

export default function App() {
  const businessId = localStorage.getItem("business_id") || "";
  const [searchOpenTick, setSearchOpenTick] = useState(0);
  const [showShortcuts, setShowShortcuts] = useState(false);
  const { theme, toggleTheme } = useTheme();

  useKeyboardShortcuts(
    (url) => {
      window.location.href = url;
    },
    {
      openSearch: () => setSearchOpenTick((n) => n + 1),
      showShortcuts: () => setShowShortcuts(true),
      closeModal: () => setShowShortcuts(false),
    }
  );

  return (
    <div className="min-h-screen bg-white text-gray-900 dark:bg-gray-900 dark:text-gray-100">
      <header className="flex items-center justify-between border-b p-3 dark:border-gray-700">
        <div className="font-semibold">RapidXAI</div>
        <div className="flex items-center gap-2">
          <button className="rounded border px-2 py-1 text-sm" onClick={toggleTheme}>
            {theme === "dark" ? "Sun" : "Moon"}
          </button>
          <NotificationCenter businessId={businessId} />
        </div>
      </header>

      <main className="p-4">
        <p className="text-sm text-gray-600 dark:text-gray-300">Use sidebar routes to navigate pages.</p>
      </main>

      {/* re-mount toggler for cmd/ctrl+k */}
      <GlobalSearch key={searchOpenTick} businessId={businessId} />
      <ShortcutsHelp open={showShortcuts} onClose={() => setShowShortcuts(false)} />
      <InstallPrompt />
    </div>
  );
}
