import { NavLink } from "react-router-dom";
import { sidebarItems } from "../navigation/sidebarItems";

type SidebarProps = {
  open: boolean;
  onClose: () => void;
};

export default function Sidebar({ open, onClose }: SidebarProps) {
  return (
    <>
      {open ? <button className="fixed inset-0 z-20 bg-black/30 lg:hidden" onClick={onClose} aria-label="Close navigation" /> : null}
      <aside
        className={[
          "fixed inset-y-0 left-0 z-30 w-64 transform border-r bg-white p-4 transition-transform dark:border-gray-700 dark:bg-gray-900 lg:static lg:translate-x-0",
          open ? "translate-x-0" : "-translate-x-full",
        ].join(" ")}
      >
        <div className="mb-4 text-lg font-semibold">Navigation</div>
        <nav className="space-y-1">
          {sidebarItems.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              onClick={onClose}
              className={({ isActive }) =>
                [
                  "block rounded px-3 py-2 text-sm transition-colors",
                  isActive
                    ? "bg-gray-900 text-white dark:bg-gray-100 dark:text-gray-900"
                    : "text-gray-700 hover:bg-gray-100 dark:text-gray-200 dark:hover:bg-gray-800",
                ].join(" ")
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </aside>
    </>
  );
}
