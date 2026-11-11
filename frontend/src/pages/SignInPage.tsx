import { FormEvent, useState } from "react";
import { ApiError } from "../api/client";
import { useAuth } from "../auth/AuthContext";

export function SignInPage() {
  const { signIn, authNotice } = useAuth();
  const [apiKey, setApiKey] = useState("");
  const [revealKey, setRevealKey] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await signIn(apiKey);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(
          err.status === 401
            ? "Invalid, expired, or revoked API key."
            : err.message,
        );
      } else {
        setError("Unable to verify API key.");
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="signin-backdrop">
      <div className="signin-card">
        <h1 style={{ margin: "0 0 6px", fontSize: 22 }}>TaskForge Web Console</h1>
        <p style={{ margin: "0 0 18px", color: "var(--text-secondary)", fontSize: 14 }}>
          Authenticate with a TaskForge API key. Credentials are held exclusively in
          React memory and never stored in browser storage or cookies.
        </p>

        {authNotice ? (
          <div className="banner banner-warn" role="status" style={{ marginBottom: 14 }}>
            <span>{authNotice}</span>
          </div>
        ) : null}

        {error ? (
          <div className="banner banner-error" role="alert" style={{ marginBottom: 14 }}>
            <span>{error}</span>
          </div>
        ) : null}

        <form onSubmit={handleSubmit}>
          <div className="field">
            <label htmlFor="api-key-input">API Key</label>
            <div style={{ display: "flex", gap: 8 }}>
              <input
                id="api-key-input"
                type={revealKey ? "text" : "password"}
                className="input mono"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder="tf_live_..."
                autoComplete="off"
                spellCheck={false}
                required
              />
              <button
                type="button"
                className="btn"
                onClick={() => setRevealKey((v) => !v)}
              >
                {revealKey ? "Hide" : "Show"}
              </button>
            </div>
            <span className="field-hint">
              Closing or refreshing the tab clears the credential automatically.
            </span>
          </div>

          <button
            type="submit"
            className="btn btn-primary"
            style={{ width: "100%" }}
            disabled={submitting || !apiKey.trim()}
          >
            {submitting ? "Verifying Key..." : "Sign In"}
          </button>
        </form>
      </div>
    </div>
  );
}

