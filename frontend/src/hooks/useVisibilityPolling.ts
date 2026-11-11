import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "../api/client";

export interface PollingOptions {
  enabled: boolean;
  intervalMs?: number;
  resetKey?: string | number;
  fetcher: () => Promise<void>;
}

export function useVisibilityPolling({
  enabled,
  intervalMs = 2500,
  resetKey,
  fetcher,
}: PollingOptions) {
  const [backoffSeconds, setBackoffSeconds] = useState<number | null>(null);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const activeRef = useRef(false);
  const inFlightRef = useRef(false);
  const timerRef = useRef<number | null>(null);

  const clearScheduled = useCallback(() => {
    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  }, []);

  useEffect(() => {
    activeRef.current = enabled;
    if (!enabled) {
      clearScheduled();
      setBackoffSeconds(null);
      return;
    }

    let cancelled = false;

    const scheduleNext = (delayMs: number) => {
      clearScheduled();
      if (cancelled || !activeRef.current) {
        return;
      }
      if (typeof document !== "undefined" && document.visibilityState === "hidden") {
        return;
      }
      timerRef.current = window.setTimeout(() => {
        void runCycle();
      }, delayMs);
    };

    const runCycle = async () => {
      if (cancelled || !activeRef.current) {
        return;
      }
      if (typeof document !== "undefined" && document.visibilityState === "hidden") {
        return;
      }
      if (inFlightRef.current) {
        scheduleNext(intervalMs);
        return;
      }

      inFlightRef.current = true;
      let nextDelay = intervalMs;
      try {
        await fetcherRef.current();
        if (!cancelled) {
          setBackoffSeconds(null);
        }
      } catch (err) {
        if (err instanceof ApiError) {
          if (err.status === 401) {
            // Stop immediately on 401; never schedule further polls with invalid credentials.
            activeRef.current = false;
            clearScheduled();
            inFlightRef.current = false;
            return;
          }
          if ((err.status === 429 || err.status === 503) && err.retryAfterSeconds) {
            nextDelay = err.retryAfterSeconds * 1000;
            if (!cancelled) {
              setBackoffSeconds(err.retryAfterSeconds);
            }
          }
        }
      } finally {
        inFlightRef.current = false;
      }

      if (!cancelled && activeRef.current) {
        scheduleNext(nextDelay);
      }
    };

    const onVisibilityChange = () => {
      if (cancelled || !activeRef.current) {
        return;
      }
      if (document.visibilityState === "hidden") {
        clearScheduled();
      } else {
        void runCycle();
      }
    };

    if (typeof document !== "undefined") {
      document.addEventListener("visibilitychange", onVisibilityChange);
    }

    void runCycle();

    return () => {
      cancelled = true;
      activeRef.current = false;
      clearScheduled();
      if (typeof document !== "undefined") {
        document.removeEventListener("visibilitychange", onVisibilityChange);
      }
    };
  }, [enabled, intervalMs, resetKey, clearScheduled]);

  return { backoffSeconds };
}

