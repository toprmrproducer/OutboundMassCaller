import { useEffect, useState } from "react";

type BeforeInstallPromptEvent = Event & {
  prompt: () => Promise<void>;
  userChoice: Promise<{ outcome: "accepted" | "dismissed" }>;
};

export default function InstallPrompt() {
  const [evt, setEvt] = useState<BeforeInstallPromptEvent | null>(null);
  const [dismissed, setDismissed] = useState(() => localStorage.getItem("pwa_install_dismissed") === "1");

  useEffect(() => {
    const handler = (e: Event) => {
      e.preventDefault();
      setEvt(e as BeforeInstallPromptEvent);
    };
    window.addEventListener("beforeinstallprompt", handler as EventListener);
    return () => window.removeEventListener("beforeinstallprompt", handler as EventListener);
  }, []);

  if (!evt || dismissed) return null;

  return (
    <div className="fixed bottom-4 left-4 right-4 z-50 rounded-lg border bg-white p-3 shadow-lg sm:left-auto sm:right-4 sm:w-96">
      <div className="text-sm font-medium">Add to Home Screen for the best experience</div>
      <div className="mt-2 flex gap-2">
        <button
          className="rounded bg-gray-900 px-3 py-1.5 text-sm text-white"
          onClick={async () => {
            await evt.prompt();
            const choice = await evt.userChoice;
            if (choice.outcome === "accepted") setEvt(null);
          }}
        >
          Install
        </button>
        <button
          className="rounded border px-3 py-1.5 text-sm"
          onClick={() => {
            localStorage.setItem("pwa_install_dismissed", "1");
            setDismissed(true);
          }}
        >
          Dismiss
        </button>
      </div>
    </div>
  );
}
