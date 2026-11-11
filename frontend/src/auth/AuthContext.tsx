import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from "react";
import { ApiError, TaskForgeClient } from "../api/client";
import type { AuthPrincipal } from "../api/types";

interface AuthContextValue {
  isAuthenticated: boolean;
  principal: AuthPrincipal | null;
  authNotice: string | null;
  forbiddenNotice: string | null;
  sessionEpoch: number;
  client: TaskForgeClient;
  signIn: (apiKey: string) => Promise<void>;
  signOut: (notice?: string) => void;
  clearForbiddenNotice: () => void;
  hasScope: (scope: string) => boolean;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  // Strict in-memory credential storage only — never persisted to Web Storage or cookies.
  const tokenRef = useRef<string | null>(null);
  const [principal, setPrincipal] = useState<AuthPrincipal | null>(null);
  const [authNotice, setAuthNotice] = useState<string | null>(null);
  const [forbiddenNotice, setForbiddenNotice] = useState<string | null>(null);
  const [sessionEpoch, setSessionEpoch] = useState(0);

  const clearSession = useCallback((notice: string | null) => {
    tokenRef.current = null;
    setPrincipal(null);
    setForbiddenNotice(null);
    setAuthNotice(notice);
    setSessionEpoch((n) => n + 1);
  }, []);

  const handleUnauthorized = useCallback(() => {
    if (tokenRef.current !== null) {
      clearSession(
        "Your API key was rejected (expired, revoked, or invalid). In-memory credentials were cleared.",
      );
    }
  }, [clearSession]);

  const handleForbidden = useCallback((err: ApiError) => {
    setForbiddenNotice(
      `Authorization error (403): ${err.message}. Your authenticated session remains active.`,
    );
  }, []);

  const client = useMemo(
    () =>
      new TaskForgeClient({
        getToken: () => tokenRef.current,
        onUnauthorized: handleUnauthorized,
        onForbidden: handleForbidden,
      }),
    [handleUnauthorized, handleForbidden],
  );

  const signIn = useCallback(
    async (rawApiKey: string) => {
      const trimmed = rawApiKey.trim();
      if (!trimmed) {
        throw new ApiError("Please enter an API key.", 400, "MISSING_KEY");
      }
      const verified = await client.getAuthMe(trimmed);
      tokenRef.current = trimmed;
      setPrincipal(verified);
      setAuthNotice(null);
      setForbiddenNotice(null);
      setSessionEpoch((n) => n + 1);
    },
    [client],
  );

  const signOut = useCallback(
    (notice?: string) => {
      clearSession(notice ?? null);
    },
    [clearSession],
  );

  const clearForbiddenNotice = useCallback(() => {
    setForbiddenNotice(null);
  }, []);

  const hasScope = useCallback(
    (scope: string) => Boolean(principal?.scopes.includes(scope)),
    [principal],
  );

  const value = useMemo<AuthContextValue>(
    () => ({
      isAuthenticated: principal !== null && tokenRef.current !== null,
      principal,
      authNotice,
      forbiddenNotice,
      sessionEpoch,
      client,
      signIn,
      signOut,
      clearForbiddenNotice,
      hasScope,
    }),
    [
      principal,
      authNotice,
      forbiddenNotice,
      sessionEpoch,
      client,
      signIn,
      signOut,
      clearForbiddenNotice,
      hasScope,
    ],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error("useAuth must be used inside an AuthProvider");
  }
  return ctx;
}

