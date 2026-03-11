import {
  Archive,
  ArrowUpRight,
  Clock3,
  Database,
  Download,
  LoaderCircle,
  MessageSquareText,
  Sparkles,
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
  latest_zip: string;
};

type ToastState = {
  tone: "success" | "error" | "info";
  message: string;
};

const metricCards = [
  {
    key: "memories_count",
    label: "Memories",
    icon: Sparkles,
  },
  {
    key: "chats_count",
    label: "Chats",
    icon: MessageSquareText,
  },
  {
    key: "messages_count",
    label: "Messages",
    icon: Archive,
  },
  {
    key: "users_count",
    label: "Users",
    icon: Database,
  },
] as const;

export default function Dashboard() {
  const [status, setStatus] = useState<IngestStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);

  useEffect(() => {
    void refreshStatus();
  }, []);

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

  async function refreshStatus() {
    try {
      setLoading(true);
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
      setLoading(false);
    }
  }

  async function triggerImport() {
    try {
      setImporting(true);
      setToast({
        tone: "info",
        message: "Import started. Waiting for backend response...",
      });

      const response = await fetch("/api/ingest/", {
        method: "POST",
      });
      const payload = (await response.json()) as { detail?: string; zip_file?: string };

      if (!response.ok) {
        throw new Error(payload.detail || `Import failed with ${response.status}`);
      }

      setToast({
        tone: "success",
        message: payload.zip_file
          ? `Import completed for ${payload.zip_file}.`
          : "Import completed successfully.",
      });

      await refreshStatus();
    } catch (requestError) {
      setToast({
        tone: "error",
        message:
          requestError instanceof Error
            ? requestError.message
            : "Import failed unexpectedly.",
      });
    } finally {
      setImporting(false);
    }
  }

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
                Premium home-lab shell for your Snapchat archive.
              </h1>
              <p className="max-w-2xl text-sm leading-7 text-slate-300">
                This dashboard is the control surface for imports and archive
                health. It is intentionally shaped like a serious self-hosted app,
                not a generic starter screen.
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-3">
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
                <span>{importing ? "Importing..." : "Import Data"}</span>
              </button>

              <button
                type="button"
                onClick={() => void refreshStatus()}
                disabled={loading}
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
                  status?.ingestion_running
                    ? "bg-amber-400/15 text-amber-200"
                    : "bg-emerald-400/15 text-emerald-200",
                ].join(" ")}
              >
                {status?.ingestion_running ? "Running" : "Idle"}
              </span>
            </div>

            <div className="mt-5 space-y-4 text-sm">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <p className="text-slate-400">Latest ZIP</p>
                <p className="mt-2 truncate text-base text-white">
                  {status?.latest_zip || "No ZIP detected in imports directory"}
                </p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="flex items-center gap-2 text-slate-400">
                  <Clock3 className="h-4 w-4" />
                  <span>Archive readiness</span>
                </div>
                <p className="mt-2 text-base text-white">
                  {status?.has_data
                    ? "Archive data is available and ready for browsing."
                    : "No parsed archive data yet. Import a Snapchat export to begin."}
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

      <section className="grid gap-6 lg:grid-cols-[0.85fr_1.15fr]">
        <article className="rounded-[1.75rem] border border-white/10 bg-slate-950/70 p-6">
          <p className="text-xs font-semibold uppercase tracking-[0.25em] text-slate-500">
            Backend Overview
          </p>
          <dl className="mt-5 space-y-5 text-sm">
            <div className="flex items-center justify-between gap-4 border-b border-white/5 pb-3">
              <dt className="text-slate-400">Imports Directory</dt>
              <dd className="truncate text-right text-cyan-200">
                {status?.imports_dir || "Unavailable"}
              </dd>
            </div>
            <div className="flex items-center justify-between gap-4 border-b border-white/5 pb-3">
              <dt className="text-slate-400">Has Data</dt>
              <dd>{status?.has_data ? "Yes" : "No"}</dd>
            </div>
            <div className="flex items-center justify-between gap-4">
              <dt className="text-slate-400">Ingestion Running</dt>
              <dd>{status?.ingestion_running ? "Yes" : "No"}</dd>
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
            {JSON.stringify(status ?? { loading: true }, null, 2)}
          </pre>
        </article>
      </section>
    </div>
  );
}
