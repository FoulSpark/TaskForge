# Phase 10 — Frontend Design System Specification (`F0`)

**Scope:** Visual Language, Color & Status Tokens, Typography, Layout Shell, UX State Matrix & Accessibility Standards  
**Design Philosophy:** Clean, high-signal, developer-first operations console inspired by structured workflow/queue dashboards—optimized for clarity under live state transitions without decorative bloat.  

---

## 1. Visual Identity & Color System

### 1.1 Surface & Text Tokens (Light Operations Theme Default)
- **App Canvas:** `#F8FAFC` (`slate-50`)
- **Card / Panel Surface:** `#FFFFFF` (`white`) with `1px solid #E2E8F0` (`slate-200`) border and `8px` radius
- **Sidebar / Nav Surface:** `#0F172A` (`slate-900`) with `#F8FAFC` primary ink and `#94A3B8` muted ink
- **Primary Text:** `#0F172A` (`slate-900`) — contrast ratio `> 15:1` on white
- **Secondary / Metadata Text:** `#475569` (`slate-600`) — contrast ratio `> 7:1` on white
- **Primary Interactive Accent:** `#2563EB` (`blue-600`, hover `#1D4ED8`)

### 1.2 Semantic Status Tokens (All 8 `TaskStatus` Values)
Status badges **always combine color + icon + text label** (never rely on color alone for WCAG compliance):

| `TaskStatus` | Category | Background | Text & Icon Color | Border | Icon Indicator |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`PENDING`** | Staged / Outbox | `#F1F5F9` | `#475569` | `#CBD5E1` | Clock / Outlined dot |
| **`QUEUED`** | Ready in Redis | `#EFF6FF` | `#1D4ED8` | `#BFDBFE` | Queue / Stacked bars |
| **`RUNNING`** | Active Execution | `#EEF2FF` | `#4338CA` | `#C7D2FE` | Animated spinner / Pulse |
| **`RETRY_WAIT`** | Backoff Scheduled | `#FFFBEB` | `#B45309` | `#FDE68A` | Refresh-wait icon |
| **`SUCCESS`** | Terminal OK | `#ECFDF5` | `#047857` | `#A7F3D0` | Check circle |
| **`FAILED`** | Retries Exhausting | `#FEF2F2` | `#B91C1C` | `#FECACA` | Alert circle |
| **`DEAD_LETTER`** | Terminal DLQ | `#FFF1F2` | `#9F1239` | `#FDA4AF` | X-octagon / Skull |
| **`CANCELLED`** | Terminal Cancelled | `#F8FAFC` | `#64748B` | `#CBD5E1` | Ban / Slash circle |

### 1.3 Priority Badges (`TaskPriority`)
- `4 CRITICAL`: `#9F1239` on `#FFE4E6`
- `3 HIGH`: `#C2410C` on `#FFEDD5`
- `2 MEDIUM`: `#1D4ED8` on `#DBEAFE`
- `1 LOW`: `#475569` on `#F1F5F9`

---

## 2. Typography & Numeric Precision

- **UI Sans:** `'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif`
- **Data Monospace:** `'JetBrains Mono', 'Fira Code', ui-monospace, SFMono-Regular, monospace`
- **Tabular Numerals:** All KPI cards, durations (`ms`), timestamps (ISO/UTC), queue depths, and pagination counters enforce `font-variant-numeric: tabular-nums` so numbers do not jitter during 2.5s polling refreshes.
- **UUID Truncation:** Task IDs render as monospace truncated links (`a4f8b910...`) with full UUID tooltip + one-click Copy button.

---

## 3. Navigation Shell & Responsive Grid

- **Top Status Bar (`56px` height):**
  - Left: `TASKFORGE` wordmark + active section breadcrumb
  - Right: Live **System Health Pill** (`Healthy` green / `Degraded` amber / `Offline` red from `/health/ready`), **Tenant & Scope Badge** (e.g. `tenant: default · read/write`), **Live Polling Pause/Resume Toggle**, and **Sign Out** button.
- **Left Sidebar (`240px` desktop, collapsible drawer `< 1024px`):**
  1. Overview (`/`)
  2. Submit Task (`/submit`) — disabled/hidden notice if key lacks `tasks:write`
  3. Task Explorer (`/tasks`)
  4. System Monitoring (`/monitoring`)
  5. *(Conditional on `admin:operator`)* Operator Controls section inside System Monitoring
- **Responsive Breakpoints:**
  - `>= 1280px`: Full persistent sidebar + 4-column KPI grid + side-by-side detail panels
  - `768px – 1279px`: Collapsible sidebar + 2-column KPI grid
  - `< 768px`: Mobile drawer menu + single-column cards + horizontally scrollable tables

---

## 4. Mandatory UX States & Edge Cases (`F7` Enforcement)

| Scenario | Required UI Behavior |
| :--- | :--- |
| **Initial Load** | Render structured skeleton placeholders matching card/table geometry (no layout shift). |
| **Empty Tenant / Filter Match** | Render helpful empty state explaining active filters with a quick "Clear Filters" or "Submit First Task" CTA. |
| **API Unavailable / Network Timeout / `503`** | Display persistent top alert banner (`Service Unavailable / Rate-Limiter Fail-Closed`), pause background polling, show last successful timestamp & exponential retry timer, retain already loaded read data rather than blanking the screen. |
| **Rate Limit `429 Too Many Requests`** | Display countdown banner sourced from `Retry-After` header; automatically resume polling only after cooldown expires. |
| **Terminal Task Mutability Guard** | Tasks in `SUCCESS` or `CANCELLED` render **read-only**; Cancel button disabled/omitted for terminal states (`DEAD_LETTER` permits **Manual Retry** only if `tasks:write` scope is present). |
| **Malformed JSON in Submit Form** | Inline client-side JSON parse diagnostic pointing out syntax error before disabling Submit button; server `422` validation errors mapped cleanly by field. |

---

## 5. Accessibility Requirements (`F7`)

- Semantic HTML landmarks (`<nav>`, `<main>`, `<header>`, `<button>`, `<table>`).
- Full keyboard navigation across sidebar links, table pagination, filters, modal Sign-In Gate, and JSON copy buttons with visible `:focus-visible` outlines.
- `aria-live="polite"` regions for task status transitions and toast feedback (`Task Created`, `Task Cancelled`, `Idempotency Key Reused`).
- Zero color-only status encoding (every badge carries distinct icon + label).

