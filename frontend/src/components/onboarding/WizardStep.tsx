import { ReactNode } from "react";

type Props = {
  title: string;
  subtitle?: string;
  children: ReactNode;
  actions?: ReactNode;
};

export default function WizardStep({ title, subtitle, children, actions }: Props) {
  return (
    <div className="rounded-2xl border bg-white p-5 shadow-sm dark:border-gray-700 dark:bg-gray-900">
      <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100">{title}</h2>
      {subtitle ? <p className="mt-1 text-sm text-gray-600 dark:text-gray-300">{subtitle}</p> : null}
      <div className="mt-4 space-y-3">{children}</div>
      {actions ? <div className="mt-5 flex flex-wrap gap-2">{actions}</div> : null}
    </div>
  );
}
