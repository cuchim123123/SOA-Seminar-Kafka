import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import { App } from './App';

window.addEventListener('error', (event) => {
  // ResizeObserver loop completed with undelivered notifications is a benign browser notification
  // triggered when @xyflow/react canvas resizes while measuring DOM dimensions. It is not an error.
  if (
    event.message?.includes('ResizeObserver') ||
    event.error?.message?.includes('ResizeObserver')
  ) {
    event.stopImmediatePropagation?.();
    return;
  }

  console.error('[Root Error Listener]', event.error || event.message);
  const root = document.getElementById('root');
  if (root && root.children.length === 0) {
    root.innerHTML = `
      <div style="padding: 24px; font-family: monospace; background: #fef2f2; border: 2px solid #ef4444; border-radius: 8px; margin: 20px;">
        <h2 style="color: #b91c1c; margin-bottom: 8px;">Frontend Initialization Error</h2>
        <div style="color: #991b1b; font-weight: bold; margin-bottom: 12px;">${event.message}</div>
        <pre style="white-space: pre-wrap; font-size: 12px; color: #7f1d1d; background: #fee2e2; padding: 12px; border-radius: 4px;">${event.error?.stack || 'No stack trace available'}</pre>
      </div>
    `;
  }
});

const rootElement = document.getElementById('root');
if (rootElement) {
  try {
    const root = createRoot(rootElement);
    root.render(
      <StrictMode>
        <App />
      </StrictMode>,
    );
  } catch (err: unknown) {
    const errorText = err instanceof Error ? err.stack || err.message : String(err);
    rootElement.innerHTML = `
      <div style="padding: 24px; font-family: monospace; background: #fef2f2; border: 2px solid #ef4444; border-radius: 8px; margin: 20px;">
        <h2 style="color: #b91c1c; margin-bottom: 8px;">React createRoot Render Error</h2>
        <pre style="white-space: pre-wrap; font-size: 12px; color: #7f1d1d;">${errorText}</pre>
      </div>
    `;
  }
}
