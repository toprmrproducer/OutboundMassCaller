import { useState } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import GlobalSearch from "./components/GlobalSearch";
import InstallPrompt from "./components/InstallPrompt";
import NotificationCenter from "./components/NotificationCenter";
import Sidebar from "./components/Sidebar";
import ShortcutsHelp from "./components/ShortcutsHelp";
import { useKeyboardShortcuts } from "./hooks/useKeyboardShortcuts";
import { useTheme } from "./hooks/useTheme";
import ActivityFeed from "./pages/ActivityFeed";
import AgentSimulator from "./pages/AgentSimulator";
import Analytics from "./pages/Analytics";
import BookingCalendar from "./pages/BookingCalendar";
import CampaignsPage from "./pages/Campaigns";
import DNCRegistry from "./pages/DNCRegistry";
import LeadsPage from "./pages/Leads";
import LiveMonitor from "./pages/LiveMonitor";
import NumberHealth from "./pages/NumberHealth";
import Onboarding from "./pages/Onboarding";
import Settings from "./pages/Settings";

export default function App() {
  const businessId = localStorage.getItem("business_id") || "";
  const [searchOpenTick, setSearchOpenTick] = useState(0);
  const [showShortcuts, setShowShortcuts] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(false);
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
    <div className="min-h-screen bg-gray-50 text-gray-900 dark:bg-gray-950 dark:text-gray-100">
      <div className="flex min-h-screen">
        <Sidebar open={sidebarOpen} onClose={() => setSidebarOpen(false)} />
        <div className="min-w-0 flex-1">
          <header className="sticky top-0 z-10 flex items-center justify-between border-b bg-white p-3 dark:border-gray-700 dark:bg-gray-900">
            <div className="flex items-center gap-2">
              <button
                className="rounded border px-2 py-1 text-sm lg:hidden"
                onClick={() => setSidebarOpen((v) => !v)}
                aria-label="Toggle navigation"
              >
                Menu
              </button>
              <div className="font-semibold">RapidXAI</div>
            </div>
            <div className="flex items-center gap-2">
              <button className="rounded border px-2 py-1 text-sm" onClick={toggleTheme}>
                {theme === "dark" ? "Sun" : "Moon"}
              </button>
              <NotificationCenter businessId={businessId} />
            </div>
          </header>

          <main className="p-4">
            <Routes>
              <Route path="/" element={<Navigate to="/dashboard" replace />} />
              <Route path="/dashboard" element={<Navigate to="/live-monitor" replace />} />
              <Route path="/onboarding" element={<Onboarding />} />
              <Route path="/analytics" element={<Analytics />} />
              <Route path="/live-monitor" element={<LiveMonitor />} />
              <Route path="/activity-feed" element={<ActivityFeed />} />
              <Route path="/dnc-registry" element={<DNCRegistry />} />
              <Route path="/agent-simulator" element={<AgentSimulator />} />
              <Route path="/number-health" element={<NumberHealth />} />
              <Route path="/bookings-calendar" element={<BookingCalendar />} />
              <Route path="/campaigns" element={<CampaignsPage />} />
              <Route path="/leads" element={<LeadsPage />} />
              <Route path="/settings" element={<Settings />} />
              <Route path="*" element={<Navigate to="/dashboard" replace />} />
            </Routes>
          </main>
        </div>
      </div>

      {/* re-mount toggler for cmd/ctrl+k */}
      <GlobalSearch key={searchOpenTick} businessId={businessId} />
      <ShortcutsHelp open={showShortcuts} onClose={() => setShowShortcuts(false)} />
      <InstallPrompt />
    </div>
  );
}
