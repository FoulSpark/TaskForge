import { useCallback, useState } from "react";
import { NavLink, Outlet } from "react-router-dom";
import type { HealthReadinessResponse } from "../api/types";
import { useAuth } from "../auth/AuthContext";
import { useVisibilityPolling } from "../hooks/useVisibilityPolling";

export function ShellLayout() {
  const {
    principal,
    isAuthenticated,
    signOut,
    forbiddenNotice,
    clearForbiddenNotice,
    client,
    sessionEpoch,
  } = useAuth();

  const [health, setHealth] = useState<HealthReadinessResponse | null>(null);
  const [healthError, setHealthError] = useState(false);

  const pollHealth = useCallback(async () => {
    try {
      const data = await client.getHealthReady();
      setHealth(data);
      setHealthError(false);
    } catch {
      setHealthError(true);
    }
  }, [client]);

  useVisibilityPolling({
    enabled: isAuthenticated,
    intervalMs: 10000,
    resetKey: sessionEpoch,
    fetcher: pollHealth,
  });

  const healthLabel = healthError
    ? "Health Unreachable"
    : health?.status === "ready" || health?.status === "healthy"
      ? "System Healthy"
      : health
        ? `System ${health.status}`
        : "Checking Health";

  const healthPillClass = healthError
    ? "pill-down"
    : health?.status === "ready" || health?.status === "healthy"
      ? "pill-healthy"
      : "pill-degraded";

  return (
    <div className="shell-layout">
      <aside className="sidebar" aria-label="Primary Navigation">
        <div className="brand">
          <div className="brand-title">
            <span aria-hidden="true">⚡</span>
            <span>TASKFORGE</span>
          </div>
          <div className="brand-sub">Distributed Task Console</div>
        </div>

        <nav className="nav-list">
          <NavLink
            to="/"
            end
            className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}
          >
            Overview
          </NavLink>
          <NavLink
            to="/submit"
            className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}
          >
            Submit Task
          </NavLink>
          <NavLink
            to="/tasks"
            className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}
          >
            Task Explorer
          </NavLink>
          <NavLink
            to="/monitoring"
            className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}
          >
            Monitoring
          </NavLink>
        </nav>

        <div className="sidebar-footer">
          <div>
            Tenant:{" "}
            <strong style={{ color: "#f8fafc" }}>{principal?.tenant_id}</strong>
          </div>
          <div>
            Key: <span className="mono">{principal?.name}</span>
          </div>
          <div>In-memory session only</div>
        </div>
      </aside>

      <div className="main-area">
        <header className="topbar">
          <div className="topbar-meta">
            <span className={`pill ${healthPillClass}`} role="status">
              ● {healthLabel}
            </span>
            <span className="pill pill-neutral">
              Tenant: {principal?.tenant_id}
            </span>
            {principal?.scopes.includes("admin:operator") ? (
              <span className="pill" style={{ background: "#ede9fe", color: "#5b21b6" }}>
                Operator Scope
              </span>
            ) : null}
          </div>

          <button
            type="button"
            className="btn btn-sm"
            onClick={() => signOut("Signed out. In-memory API key cleared.")}
          >
            Sign Out
          </button>
        </header>

        <main className="page-content">
          {forbiddenNotice ? (
            <div className="banner banner-warn" role="alert">
              <span>{forbiddenNotice}</span>
              <button
                type="button"
                className="btn btn-sm"
                onClick={clearForbiddenNotice}
              >
                Dismiss
              </button>
            </div>
          ) : null}
          <Outlet />
        </main>
      </div>
    </div>
  );
}

