import { ReactNode } from "react";

export type BulkAction = {
  label: string;
  icon?: ReactNode;
  variant?: "default" | "destructive";
  onClick: (ids: string[]) => void;
};

type Props = {
  selectedIds: string[];
  actions: BulkAction[];
  onClear: () => void;
};

export default function BulkActionBar({ selectedIds, actions, onClear }: Props) {
  if (!selectedIds.length) return null;
  return (
    <div className="fixed bottom-0 left-0 right-0 z-40 border-t bg-white/95 p-3 shadow-lg backdrop-blur animate-in slide-in-from-bottom">
      <div className="mx-auto flex max-w-6xl flex-wrap items-center gap-2">
        <div className="mr-2 text-sm font-medium">{selectedIds.length} selected</div>
        {actions.map((a) => (
          <button
            key={a.label}
            className={`rounded px-3 py-1.5 text-sm ${a.variant === "destructive" ? "bg-red-600 text-white" : "bg-gray-900 text-white"}`}
            onClick={() => a.onClick(selectedIds)}
          >
            {a.icon}
            {a.label}
          </button>
        ))}
        <button className="rounded border px-3 py-1.5 text-sm" onClick={onClear}>Clear</button>
      </div>
    </div>
  );
}
