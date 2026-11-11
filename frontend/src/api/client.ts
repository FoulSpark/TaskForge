import type {
  ApiErrorBody,
  AuthPrincipal,
  HealthReadinessResponse,
  MonitoringSummaryResponse,
  OperatorSystemStatsResponse,
  TaskCreateInput,
  TaskItem,
  TaskListResponse,
  TaskSubmissionOutcome,
  TaskTypesCatalogResponse,
  TenantStatsResponse,
} from "./types";

const DEFAULT_TIMEOUT_MS = 5000;

export class ApiError extends Error {
  status: number;
  code: string;
  retryAfterSeconds: number | null;
  details: unknown;

  constructor(
    message: string,
    status: number,
    code = "API_ERROR",
    retryAfterSeconds: number | null = null,
    details: unknown = undefined,
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.retryAfterSeconds = retryAfterSeconds;
    this.details = details;
  }
}

function parseRetryAfter(headerValue: string | null, status: number): number | null {
  if (status !== 429 && status !== 503) {
    return null;
  }
  if (!headerValue) {
    return 5;
  }
  const numeric = Number(headerValue);
  if (Number.isFinite(numeric) && numeric > 0) {
    return Math.ceil(numeric);
  }
  const parsedDate = Date.parse(headerValue);
  if (Number.isFinite(parsedDate)) {
    const diffSec = Math.ceil((parsedDate - Date.now()) / 1000);
    return diffSec > 0 ? diffSec : 5;
  }
  return 5;
}

export interface ApiClientHandlers {
  getToken: () => string | null;
  getSessionEpoch?: () => number;
  getSessionSignal?: () => AbortSignal | undefined;
  onUnauthorized?: () => void;
  onForbidden?: (error: ApiError) => void;
}

function buildCombinedSignal(
  timeoutMs: number,
  sessionSignal?: AbortSignal,
): { signal: AbortSignal; cleanup: () => void } {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);

  const onSessionAbort = () => controller.abort();
  if (sessionSignal) {
    if (sessionSignal.aborted) {
      controller.abort();
    } else {
      sessionSignal.addEventListener("abort", onSessionAbort, { once: true });
    }
  }

  return {
    signal: controller.signal,
    cleanup: () => {
      window.clearTimeout(timer);
      sessionSignal?.removeEventListener("abort", onSessionAbort);
    },
  };
}

export class TaskForgeClient {
  private readonly getToken: () => string | null;
  private readonly getSessionEpoch?: () => number;
  private readonly getSessionSignal?: () => AbortSignal | undefined;
  private readonly onUnauthorized?: () => void;
  private readonly onForbidden?: (error: ApiError) => void;

  constructor(handlers: ApiClientHandlers) {
    this.getToken = handlers.getToken;
    this.getSessionEpoch = handlers.getSessionEpoch;
    this.getSessionSignal = handlers.getSessionSignal;
    this.onUnauthorized = handlers.onUnauthorized;
    this.onForbidden = handlers.onForbidden;
  }

  private async requestWithStatus<T>(
    path: string,
    options: RequestInit & { tokenOverride?: string; skipAuthHeader?: boolean } = {},
  ): Promise<{ data: T; status: number }> {
    const { tokenOverride, skipAuthHeader, headers: extraHeaders, ...fetchOptions } = options;
    const token = tokenOverride ?? this.getToken();
    const requestEpoch = this.getSessionEpoch?.();
    const isStaleSession = () =>
      !tokenOverride &&
      !skipAuthHeader &&
      this.getSessionEpoch !== undefined &&
      this.getSessionEpoch() !== requestEpoch;

    const headers = new Headers(extraHeaders);
    headers.set("Accept", "application/json");
    if (fetchOptions.body && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }
    if (!skipAuthHeader && token) {
      headers.set("Authorization", `Bearer ${token}`);
    }

    const sessionSignal = !tokenOverride && !skipAuthHeader ? this.getSessionSignal?.() : undefined;
    const { signal, cleanup } = buildCombinedSignal(DEFAULT_TIMEOUT_MS, sessionSignal);

    let response: Response;
    try {
      response = await fetch(path, {
        ...fetchOptions,
        headers,
        signal,
      });
    } catch (err) {
      cleanup();
      if (isStaleSession()) {
        throw new ApiError("Session cleared while request was in flight", 401, "SESSION_INVALIDATED");
      }
      if (err instanceof DOMException && err.name === "AbortError") {
        throw new ApiError("Request timed out after 5 seconds", 0, "TIMEOUT");
      }
      throw new ApiError(
        err instanceof Error ? err.message : "Network error communicating with TaskForge API",
        0,
        "NETWORK_ERROR",
      );
    }
    cleanup();

    if (isStaleSession()) {
      throw new ApiError("Session cleared while request was in flight", 401, "SESSION_INVALIDATED");
    }

    if (!response.ok) {
      let body: ApiErrorBody | null = null;
      try {
        body = (await response.json()) as ApiErrorBody;
      } catch {
        body = null;
      }

      if (isStaleSession()) {
        throw new ApiError("Session cleared while request was in flight", 401, "SESSION_INVALIDATED");
      }

      const code = body?.error?.code ?? `HTTP_${response.status}`;
      const message =
        body?.error?.message ?? `Request failed with status ${response.status}`;
      const retryAfterSeconds = parseRetryAfter(
        response.headers.get("Retry-After"),
        response.status,
      );
      const apiErr = new ApiError(
        message,
        response.status,
        code,
        retryAfterSeconds,
        body?.error?.details,
      );

      if (response.status === 401 && !tokenOverride) {
        this.onUnauthorized?.();
      } else if (response.status === 403) {
        this.onForbidden?.(apiErr);
      }
      throw apiErr;
    }

    const parsed = (await response.json()) as T;
    if (isStaleSession()) {
      throw new ApiError("Session cleared while request was in flight", 401, "SESSION_INVALIDATED");
    }
    return { data: parsed, status: response.status };
  }

  private async request<T>(
    path: string,
    options: RequestInit & { tokenOverride?: string; skipAuthHeader?: boolean } = {},
  ): Promise<T> {
    const res = await this.requestWithStatus<T>(path, options);
    return res.data;
  }

  getAuthMe(tokenOverride?: string): Promise<AuthPrincipal> {
    return this.request<AuthPrincipal>("/api/v1/auth/me", { tokenOverride });
  }

  getHealthReady(): Promise<HealthReadinessResponse> {
    return this.request<HealthReadinessResponse>("/health/ready", {
      skipAuthHeader: true,
    });
  }

  getStats(): Promise<TenantStatsResponse> {
    return this.request<TenantStatsResponse>("/api/v1/stats");
  }

  getMonitoringSummary(windowMinutes = 5): Promise<MonitoringSummaryResponse> {
    return this.request<MonitoringSummaryResponse>(
      `/api/v1/monitoring/summary?window_minutes=${encodeURIComponent(String(windowMinutes))}`,
    );
  }

  getOperatorSystemStats(): Promise<OperatorSystemStatsResponse> {
    return this.request<OperatorSystemStatsResponse>("/api/v1/operator/system-stats");
  }

  getTaskTypes(): Promise<TaskTypesCatalogResponse> {
    return this.request<TaskTypesCatalogResponse>("/api/v1/task-types");
  }

  listTasks(params: {
    status?: string;
    task_type?: string;
    priority?: string;
    limit?: number;
    offset?: number;
  } = {}): Promise<TaskListResponse> {
    const sp = new URLSearchParams();
    if (params.status) sp.set("status", params.status);
    if (params.task_type) sp.set("task_type", params.task_type);
    if (params.priority) sp.set("priority", params.priority);
    sp.set("limit", String(params.limit ?? 20));
    sp.set("offset", String(params.offset ?? 0));
    return this.request<TaskListResponse>(`/api/v1/tasks?${sp.toString()}`);
  }

  getTask(taskId: string): Promise<TaskItem> {
    return this.request<TaskItem>(`/api/v1/tasks/${encodeURIComponent(taskId)}`);
  }

  async createTask(input: TaskCreateInput): Promise<TaskSubmissionOutcome> {
    const { data, status } = await this.requestWithStatus<TaskItem>("/api/v1/tasks", {
      method: "POST",
      body: JSON.stringify(input),
    });
    return {
      task: data,
      status,
      isReplay: status === 200,
    };
  }

  cancelTask(taskId: string): Promise<TaskItem> {
    return this.request<TaskItem>(`/api/v1/tasks/${encodeURIComponent(taskId)}/cancel`, {
      method: "POST",
    });
  }

  retryTask(taskId: string): Promise<TaskItem> {
    return this.request<TaskItem>(`/api/v1/tasks/${encodeURIComponent(taskId)}/retry`, {
      method: "POST",
    });
  }
}

