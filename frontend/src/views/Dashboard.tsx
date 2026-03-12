import {
  Archive,
  ArrowUpRight,
  Clock3,
  Database,
  Download,
  LoaderCircle,
  MessageSquareText,
  Settings2,
  Sparkles,
  StopCircle,
} from "lucide-react";
import { useEffect, useState } from "react";

type IngestStatus = {
  memories_count: number;
  chats_count: number;
  messages_count: number;
  users_count: number;
  has_data: boolean;
  ingestion_running: boolean;
  imports_dir: string;
  pending_dir: string;
  raw_media_dir: string;
  latest_zip: string;
  latest_import: string;
  latest_import_kind: string;
  queue_total: number;
  queue_pending: number;
  current_archive: string;
  current_archive_index: number;
  current_archive_total: number;
  overall_progress: number;
  current_step: string;
  download_total: number;
  download_completed: number;
  download_skipped: number;
  download_failed: number;
};

type SettingsPayload = {
  auto_import_enabled: boolean;
};

type BatchSummary = {
  success: boolean;
  cancelled: boolean;
  total_archives: number;
  processed_archives: number;
  failed_archives: number;
};

type ToastState = {
  tone: "success" | "error" | "info";
  message: string;
};

const metricCards = [
  { key: "memories_count", label: "Memories", icon: Sparkles },
  { key: "chats_count", label: "Chats", icon: MessageSquareText },
  { key: "messages_count", label: "Messages", icon: Archive },
  { key: "users_count", label: "Users", icon: Database },
] as const;

function formatPercent(value: number | undefined) {
  return `${Math.round((value ?? 0) * 100)}%`;
}

export default function Dashboard() {
  const [status, setStatus] = useState<IngestStatus | null>(null);
  const [settings, setSettings] = useState<SettingsPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [settingsLoading, setSettingsLoading] = useState(true);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [importing, setImporting] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);
  const ingestionRunning = Boolean(status?.ingestion_running) || importing;

  useEffect(() => {
    void Promise.all([refreshStatus(), refreshSettings()]);
  }, []);

  useEffect(() => {
    if (!ingestionRunning) {
      return undefined;
    }

    const timer = window.setInterval(() => {
      void refreshStatus(false);
    }, 2000);

    return () => {
      window.clearInterval(timer);
    };
  }, [ingestionRunning]);

  useEffect(() => {
    if (!toast) {
      return undefined;
    }

    const timer = window.setTimeout(() => {
      setToast(null);
    }, 3500);

    return () => {
      window.clearTimeout(timer);
    };
  }, [toast]);

  async function refreshStatus(showLoading = true) {
    try {
      if (showLoading) {
        setLoading(true);
      }
      const response = await fetch("/api/ingest/status");
      if (!response.ok) {
        throw new Error(`Status request failed with ${response.status}`);
      }

      const payload = (await response.json()) as IngestStatus;
      setStatus(payload);
      setError(null);
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Unknown request error",
      );
    } finally {
      if (showLoading) {
        setLoading(false);
      }
    }
  }

  async function refreshSettings() {
    try {
      setSettingsLoading(true);
      const response = await fetch("/api/settings/");
      if (!response.ok) {
        throw new Error(`Settings request failed with ${response.status}`);
      }
      const payload = (await response.json()) as SettingsPayload;
      setSettings(payload);
    } catch (requestError) {
      setToast({
        tone: "error",
        message:
          requestError instanceof Error
            ? requestError.message
            : "Failed to load settings.",
      });
    } finally {
      setSettingsLoading(false);
    }
  }

  async function updateAutoImport(enabled: boolean) {
    try {
      setSettingsSaving(true);
      const response = await fetch("/api/settings/", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ auto_import_enabled: enabled }),
      });
      if (!response.ok) {
        throw new Error(`Settings update failed with ${response.status}`);
      }
      const payload = (await response.json()) as SettingsPayload;
      setSettings(payload);
      setToast({
        tone: "success",
        message: payload.auto_import_enabled
          ? "Auto-import mode enabled."
          : "Auto-import mode disabled.",
      });
    } catch (requestError) {
      setToast({
        tone: "error",
        message:
          requestError instanceof Error
            ? requestError.message
            : "Failed to update settings.",
      });
    } finally {
      setSettingsSaving(false);
    }
  }

  async function triggerImport() {
    try {
      setImporting(true);
      setStatus((current) =>
        current ? { ...current, ingestion_running: true } : current,
      );
      setToast({
        tone: "info",
        message: "Batch import started. Polling backend progress...",
      });

      const response = await fetch("/api/ingest/", {
        method: "POST",
      });
      const payload = (await response.json()) as BatchSummary & { detail?: string };

      if (!response.ok) {
        throw new Error(payload.detail || `Import failed with ${response.status}`);
      }

      if (payload.success) {
        setToast({
          tone: "success",
          message: `Processed ${payload.processed_archives} archive(s) with no failures.`,
        });
      } else {
        setToast({
          tone: "error",
          message: `Processed ${payload.processed_archives} archive(s); ${payload.failed_archives} failed.`,
        });
      }
    } catch (requestError) {
      const message =
        requestError instanceof Error
          ? requestError.message
          : "Import failed unexpectedly.";

      setToast({
        tone: /cancel/i.test(message) ? "info" : "error",
        message,
      });
    } finally {
      setImporting(false);
      await refreshStatus(false);
    }
  }

  async function cancelImport() {
    try {
      setCancelling(true);
      const response = await fetch("/api/ingest/cancel", {
        method: "POST",
      });
      const payload = (await response.json()) as { detail?: string };

      if (!response.ok) {
        throw new Error(
          payload.detail || `Cancel request failed with ${response.status}`,
        );
      }

      setToast({
        tone: "info",
        message:
          payload.detail || "Cancellation requested. Waiting for import to stop.",
      });
      setStatus((current) =>
        current ? { ...current, ingestion_running: true } : current,
      );
      await refreshStatus(false);
    } catch (requestError) {
      setToast({
        tone: "error",
        message:
          requestError instanceof Error
            ? requestError.message
            : "Cancel request failed unexpectedly.",
      });
    } finally {
      setCancelling(false);
    }
  }

  const progressWidth = `${Math.max(4, Math.round((status?.overall_progress ?? 0) * 100))}%`;
  const downloadHandled =
    (status?.download_completed ?? 0) +
    (status?.download_skipped ?? 0) +
    (status?.download_failed ?? 0);
  const downloadWidth = status?.download_total
    ? `${Math.max(4, Math.round((downloadHandled / status.download_total) * 100))}%`
    : "0%";

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-6">
      <section className="overflow-hidden rounded-[2rem] border border-white/10 bg-[linear-gradient(135deg,_rgba(9,16,25,0.94),_rgba(8,28,45,0.86),_rgba(6,13,22,0.96))] shadow-2xl shadow-black/30">
        <div className="grid gap-10 px-6 py-8 md:px-8 lg:grid-cols-[1.35fr_0.85fr] lg:px-10">
          <div className="space-y-6">
            <div className="space-y-4">
              <p className="text-xs font-semibold uppercase tracking-[0.35em] text-cyan-300/75">
                Dashboard
              </p>
              <h1 className="max-w-3xl text-4xl font-semibold tracking-tight text-white md:text-5xl">
                Dropzone control surface for split Snapchat exports.
              </h1>
              <p className="max-w-2xl text-sm leading-7 text-slate-300">
                Drop exported .zip files into <span className="font-semibold text-cyan-200">/data/imports/pending</span>.
                {" "}
                The backend now processes archives as a queue, preserves raw media,
                and merges JSON plus media-only parts over repeated runs.
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              {ingestionRunning ? (
                <button
                  type="button"
                  onClick={() => void cancelImport()}
                  disabled={cancelling}
                  className="inline-flex items-center gap-2 rounded-2xl bg-rose-500 px-5 py-3 text-sm font-semibold text-white transition hover:bg-rose-400 disabled:cursor-not-allowed disabled:bg-rose-500/60"
                >
                  {cancelling ? (
                    <LoaderCircle className="h-4 w-4 animate-spin" />
                  ) : (
                    <StopCircle className="h-4 w-4" />
                  )}
                  <span>{cancelling ? "Cancelling..." : "Cancel Import"}</span>
                </button>
              ) : (
                <button
                  type="button"
                  onClick={() => void triggerImport()}
                  disabled={importing}
                  className="inline-flex items-center gap-2 rounded-2xl bg-cyan-400 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-cyan-300 disabled:cursor-not-allowed disabled:bg-cyan-500/60"
                >
                  {importing ? (
                    <LoaderCircle className="h-4 w-4 animate-spin" />
                  ) : (
                    <Download className="h-4 w-4" />
                  )}
                  <span>{importing ? "Importing..." : "Process Pending Queue"}</span>
                </button>
              )}

              <button
                type="button"
                onClick={() => void Promise.all([refreshStatus(), refreshSettings()])}
                disabled={loading || settingsLoading}
                className="inline-flex items-center gap-2 rounded-2xl border border-white/10 bg-white/5 px-5 py-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:cursor-not-allowed"
              >
                <ArrowUpRight className="h-4 w-4" />
                Refresh status
              </button>
            </div>
          </div>

          <div className="rounded-[1.75rem] border border-white/10 bg-black/20 p-5 backdrop-blur">
            <div className="flex items-center justify-between">
              <p className="text-xs font-semibold uppercase tracking-[0.25em] text-slate-400">
                Ingestion State
              </p>
              <span
                className={[
                  "rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em]",
                  ingestionRunning
                    ? "bg-amber-400/15 text-amber-200"
                    : "bg-emerald-400/15 text-emerald-200",
                ].join(" ")}
              >
                {ingestionRunning ? "Running" : "Idle"}
              </span>
            </div>

            <div className="mt-5 space-y-4 text-sm">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <p className="text-slate-400">Current Batch</p>
                <p className="mt-2 text-base text-white">
                  {status?.current_archive
                    ? `Processing archive ${status.current_archive_index} of ${status.current_archive_total}`
                    : status?.queue_pending
                      ? `${status.queue_pending} archive(s) waiting in pending`
                      : "Pending queue is empty"}
                </p>
                <p className="mt-2 truncate text-xs uppercase tracking-[0.2em] text-slate-500">
                  {status?.current_archive || status?.latest_zip || "No archive detected"}
                </p>
                <div className="mt-4 h-2 rounded-full bg-white/10">
                  <div
                    className="h-2 rounded-full bg-cyan-300 transition-all"
                    style={{ width: progressWidth }}
                  />
                </div>
                <p className="mt-2 text-xs text-slate-400">
                  {status?.current_step || "Idle"} • {formatPercent(status?.overall_progress)}
                </p>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="flex items-center gap-2 text-slate-400">
                  <Clock3 className="h-4 w-4" />
                  <span>Memory Download Progress</span>
                </div>
                <p className="mt-2 text-base text-white">
                  {status?.download_total
                    ? `${downloadHandled} of ${status.download_total} handled`
                    : "No memory downloads active"}
                </p>
                <div className="mt-4 h-2 rounded-full bg-white/10">
                  <div
                    className="h-2 rounded-full bg-emerald-300 transition-all"
                    style={{ width: downloadWidth }}
                  />
                </div>
                <p className="mt-2 text-xs text-slate-400">
                  Completed {status?.download_completed ?? 0} • Skipped {status?.download_skipped ?? 0} • Failed {status?.download_failed ?? 0}
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {toast ? (
        <div
          className={[
            "rounded-2xl border px-4 py-3 text-sm shadow-lg",
            toast.tone === "success"
              ? "border-emerald-400/25 bg-emerald-400/10 text-emerald-100"
              : toast.tone === "error"
                ? "border-rose-400/25 bg-rose-400/10 text-rose-100"
                : "border-cyan-400/25 bg-cyan-400/10 text-cyan-100",
          ].join(" ")}
        >
          {toast.message}
        </div>
      ) : null}

      {error ? (
        <div className="rounded-2xl border border-rose-400/20 bg-rose-400/10 px-4 py-3 text-sm text-rose-100">
          {error}
        </div>
      ) : null}

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {metricCards.map((card) => {
          const Icon = card.icon;
          const value = status?.[card.key] ?? 0;

          return (
            <article
              key={card.key}
              className="rounded-[1.75rem] border border-white/10 bg-slate-950/70 p-5 shadow-lg shadow-black/20"
            >
              <div className="flex items-start justify-between">
                <div>
                  <p className="text-sm text-slate-400">{card.label}</p>
                  <p className="mt-4 text-3xl font-semibold text-white">
                    {loading ? "..." : value}
                  </p>
                </div>
                <div className="rounded-2xl bg-cyan-400/10 p-3 text-cyan-200">
                  <Icon className="h-5 w-5" />
                </div>
              </div>
            </article>
          );
        })}
      </section>

      <section className="grid gap-6 lg:grid-cols-[0.95fr_1.05fr]">
        <article className="rounded-[1.75rem] border border-white/10 bg-slate-950/70 p-6">
          <div className="flex items-center gap-3">
            <div className="rounded-2xl bg-cyan-400/10 p-3 text-cyan-200">
              <Settings2 className="h-5 w-5" />
            </div>
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.25em] text-slate-500">
                Automation Settings
              </p>
              <p className="mt-1 text-sm text-slate-300">
                Poll the dropzone every 5 minutes and auto-start a batch if pending archives exist.
              </p>
            </div>
          </div>

          <div className="mt-6 flex items-center justify-between gap-4 rounded-2xl border border-white/10 bg-white/5 p-4">
            <div>
              <p className="text-sm font-medium text-white">Auto-Import Mode</p>
              <p className="mt-1 text-sm text-slate-400">
                Uses the backend watchdog on startup and every 5 minutes.
              </p>
            </div>
            <button
              type="button"
              disabled={settingsLoading || settingsSaving}
              onClick={() => void updateAutoImport(!(settings?.auto_import_enabled ?? false))}
              className={[
                "min-w-28 rounded-full px-4 py-2 text-sm font-semibold transition",
                settings?.auto_import_enabled
                  ? "bg-emerald-400 text-slate-950 hover:bg-emerald-300"
                  : "bg-white/10 text-slate-100 hover:bg-white/20",
              ].join(" ")}
            >
              {settingsSaving
                ? "Saving..."
                : settingsLoading
                  ? "Loading..."
                  : settings?.auto_import_enabled
                    ? "Enabled"
                    : "Disabled"}
            </button>
          </div>

          <dl className="mt-5 space-y-4 text-sm">
            <div className="flex items-center justify-between gap-4 border-b border-white/5 pb-3">
              <dt className="text-slate-400">Pending Queue</dt>
              <dd className="text-cyan-200">{status?.queue_pending ?? 0}</dd>
            </div>
            <div className="flex items-center justify-between gap-4 border-b border-white/5 pb-3">
              <dt className="text-slate-400">Pending Directory</dt>
              <dd className="truncate text-right text-cyan-200">
                {status?.pending_dir || "/data/imports/pending"}
              </dd>
            </div>
            <div className="flex items-center justify-between gap-4">
              <dt className="text-slate-400">Raw Media Root</dt>
              <dd className="truncate text-right text-cyan-200">
                {status?.raw_media_dir || "Unavailable"}
              </dd>
            </div>
          </dl>
        </article>

        <article className="rounded-[1.75rem] border border-white/10 bg-[#07131d] p-6">
          <div className="flex items-center justify-between">
            <p className="text-xs font-semibold uppercase tracking-[0.25em] text-slate-500">
              Raw Status Payload
            </p>
            <span className="rounded-full border border-white/10 px-3 py-1 text-xs text-slate-400">
              JSON
            </span>
          </div>
          <pre className="mt-5 overflow-x-auto rounded-2xl bg-black/30 p-5 text-sm leading-6 text-cyan-100">
            {JSON.stringify(
              {
                status: status ?? { loading: true },
                settings,
              },
              null,
              2,
            )}
          </pre>
        </article>
      </section>
    </div>
  );
}
