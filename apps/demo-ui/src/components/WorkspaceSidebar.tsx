import { useEffect, useRef, useState } from 'react';
import type { WorkspaceRoute } from '../real/state/routes';
import { routeHash } from '../real/state/routes';
import { uiConfig } from '../config/ui-config';

export function WorkspaceSidebar({ route }: { route: WorkspaceRoute }) {
  const [open, setOpen] = useState(false);
  const [compact, setCompact] = useState(false);
  const nav = useRef<HTMLElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    setOpen(false);
  }, [route.page]);
  useEffect(() => {
    if (!open) return;
    nav.current?.querySelector<HTMLElement>('button, a')?.focus();
    const keydown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setOpen(false);
        trigger.current?.focus();
      }
      if (event.key !== 'Tab') return;
      const elements = Array.from(nav.current?.querySelectorAll<HTMLElement>('a, button') ?? []);
      const first = elements[0];
      const last = elements[elements.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      }
      if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    };
    document.addEventListener('keydown', keydown);
    return () => document.removeEventListener('keydown', keydown);
  }, [open]);
  const link = (page: WorkspaceRoute['page'], label: string, real = false) => (
    <a
      onClick={() => {
        if (open) {
          setOpen(false);
          document.querySelector<HTMLElement>('main')?.focus();
        }
      }}
      key={page}
      href={routeHash({ ...route, page, real })}
      aria-current={route.page === page && route.real === real ? 'page' : undefined}
      title={label}
      aria-label={label}
    >
      <span aria-hidden="true">{label.slice(0, 1)}</span>
      <span className="sidebar-label">{label}</span>
    </a>
  );
  return (
    <>
      <button
        ref={trigger}
        className="sidebar-toggle"
        aria-expanded={open}
        aria-controls="workspace-nav"
        onClick={() => setOpen(!open)}
      >
        Menu
      </button>
      {open && (
        <button
          className="sidebar-shade"
          aria-label="Close navigation"
          onClick={() => {
            setOpen(false);
            trigger.current?.focus();
          }}
        />
      )}
      <nav
        ref={nav}
        id="workspace-nav"
        aria-label="Workspace"
        className={`workspace-sidebar ${open ? 'open' : ''} ${compact ? 'compact' : ''}`}
      >
        <strong className="sidebar-label">Kafka Streaming Lab</strong>
        <button
          onClick={() => setCompact(!compact)}
          aria-label={compact ? 'Expand navigation' : 'Collapse navigation'}
        >
          {compact ? '»' : '«'}
        </button>
        {link('simulation', 'Simulation Playground')}
        <h2 className="sidebar-label">Real Application</h2>
        {link('overview', 'Overview', true)}
        {link('purchases', 'Purchases', true)}
        {link('loyalty', 'Loyalty Accounts', true)}
        {link('analytics', 'Sales Analytics', true)}
        {link('events', 'Event Evidence', true)}
        {link('consumers', 'Consumer Controls', true)}
        {link('recovery', 'Replay & Correction', true)}
        {link('benchmark', 'Purchase Benchmark')}
        {link('guide', 'Architecture Guide')}
        <h2 className="sidebar-label">External tools</h2>
        <a
          href={uiConfig.VITE_KAFBAT_URL}
          target="_blank"
          rel="noopener noreferrer"
          aria-label="Kafbat (opens new tab)"
          title="Kafbat"
        >
          <span aria-hidden="true">K</span>
          <span className="sidebar-label">Kafbat ↗</span>
        </a>
        <a
          href={uiConfig.VITE_GRAFANA_URL}
          target="_blank"
          rel="noopener noreferrer"
          aria-label="Grafana (opens new tab)"
          title="Grafana"
        >
          <span aria-hidden="true">G</span>
          <span className="sidebar-label">Grafana ↗</span>
        </a>
      </nav>
    </>
  );
}
