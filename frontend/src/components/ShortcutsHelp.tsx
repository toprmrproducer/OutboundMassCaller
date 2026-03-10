import { SHORTCUTS } from "../hooks/useKeyboardShortcuts";

type Props = { open: boolean; onClose: () => void };

export default function ShortcutsHelp({ open, onClose }: Props) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 bg-black/40 p-4" onClick={onClose}>
      <div className="mx-auto max-w-xl rounded-xl border bg-white p-4 shadow-lg" onClick={(e) => e.stopPropagation()}>
        <div className="mb-3 flex items-center justify-between">
          <h3 className="text-lg font-semibold">Keyboard Shortcuts</h3>
          <button className="rounded border px-2 py-1" onClick={onClose}>Close</button>
        </div>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
          {SHORTCUTS.map((s) => (
            <div key={s.action} className="flex items-center justify-between rounded border px-2 py-1 text-sm">
              <span>{s.keys[0]}</span>
              <span className="text-gray-600">{s.label}</span>
            </div>
          ))}
        </div>
        <p className="mt-3 text-xs text-gray-500">Keyboard shortcuts are available on all pages.</p>
      </div>
    </div>
  );
}
