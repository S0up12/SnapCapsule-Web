import {
  Clapperboard,
  Expand,
  Heart,
  ImageIcon,
  LoaderCircle,
  X,
} from "lucide-react";
import { useEffect, useState } from "react";

type MemoryItem = {
  asset_id: string;
  file_type: string;
  year: string | null;
  is_favorite: boolean;
  media_url: string | null;
  thumbnail_url: string | null;
  overlay_url: string | null;
};

type MemoriesResponse = {
  items: MemoryItem[];
  skip: number;
  limit: number;
  total: number;
};

const PAGE_SIZE = 50;

function isVideo(memory: MemoryItem) {
  if ((memory.file_type || "").toLowerCase() === "video") {
    return true;
  }

  const target = memory.media_url || memory.thumbnail_url || "";
  return /\.(mp4|mov|avi|webm|m4v)(\?|$)/i.test(target);
}

function formatMemoryLabel(memory: MemoryItem) {
  const candidates = [memory.asset_id, memory.media_url || "", memory.thumbnail_url || ""];

  for (const value of candidates) {
    const dateMatch = value.match(/(20\d{2})[-_](\d{2})[-_](\d{2})(?:[-_ T](\d{2})[-_:]?(\d{2}))?/);
    if (!dateMatch) {
      continue;
    }

    const [, year, month, day, hour, minute] = dateMatch;
    const date = new Date(
      Number(year),
      Number(month) - 1,
      Number(day),
      hour ? Number(hour) : 12,
      minute ? Number(minute) : 0,
    );

    if (Number.isNaN(date.getTime())) {
      continue;
    }

    return new Intl.DateTimeFormat(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
      ...(hour
        ? {
            hour: "numeric",
            minute: "2-digit",
          }
        : {}),
    }).format(date);
  }

  if (memory.year) {
    return memory.year;
  }

  return "Undated memory";
}

function MemorySkeleton() {
  return (
    <div className="aspect-[3/4] animate-pulse rounded-[1.6rem] border border-white/10 bg-white/[0.03]" />
  );
}

export default function Memories() {
  const [memories, setMemories] = useState<MemoryItem[]>([]);
  const [selectedMemory, setSelectedMemory] = useState<MemoryItem | null>(null);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const hasMore = memories.length < total;

  useEffect(() => {
    void fetchMemories(0, false);
  }, []);

  useEffect(() => {
    if (!selectedMemory) {
      return undefined;
    }

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setSelectedMemory(null);
      }
    };

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", handleEscape);

    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", handleEscape);
    };
  }, [selectedMemory]);

  async function fetchMemories(skip: number, append: boolean) {
    try {
      if (append) {
        setLoadingMore(true);
      } else {
        setLoading(true);
      }

      const response = await fetch(`/api/memories/?skip=${skip}&limit=${PAGE_SIZE}`);
      if (!response.ok) {
        throw new Error(`Memories request failed with ${response.status}`);
      }

      const payload = (await response.json()) as MemoriesResponse;

      setMemories((current) => {
        if (!append) {
          return payload.items;
        }

        const seen = new Set(current.map((item) => item.asset_id));
        const next = [...current];
        for (const item of payload.items) {
          if (!seen.has(item.asset_id)) {
            next.push(item);
          }
        }
        return next;
      });
      setTotal(payload.total);
      setError(null);
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to load memories.",
      );
    } finally {
      setLoading(false);
      setLoadingMore(false);
    }
  }

  async function loadMore() {
    if (loadingMore || loading || !hasMore) {
      return;
    }

    await fetchMemories(memories.length, true);
  }

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-6">
      <section className="overflow-hidden rounded-[2rem] border border-white/10 bg-[linear-gradient(135deg,_rgba(8,16,28,0.98),_rgba(8,24,40,0.88),_rgba(4,9,16,0.98))] shadow-2xl shadow-black/30">
        <div className="grid gap-8 px-6 py-8 md:px-8 xl:grid-cols-[1.15fr_0.85fr] xl:px-10">
          <div className="space-y-5">
            <p className="text-xs font-semibold uppercase tracking-[0.34em] text-cyan-300/70">
              Memories
            </p>
            <div className="space-y-4">
              <h1 className="max-w-4xl text-4xl font-semibold tracking-tight text-white md:text-5xl">
                A dense gallery wall for the best parts of the archive.
              </h1>
              <p className="max-w-2xl text-sm leading-7 text-slate-300">
                Thumbnails stream in batches of 50 so the grid stays quick, even
                when the archive gets large. Click any tile for the full media
                viewer.
              </p>
            </div>
          </div>

          <div className="grid gap-4 sm:grid-cols-3 xl:grid-cols-1">
            <article className="rounded-[1.6rem] border border-white/10 bg-white/[0.045] p-5 backdrop-blur">
              <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">
                Loaded
              </p>
              <p className="mt-4 text-3xl font-semibold text-white">
                {loading ? "..." : memories.length}
              </p>
              <p className="mt-2 text-sm text-slate-400">
                Visible memories in the current session
              </p>
            </article>

            <article className="rounded-[1.6rem] border border-white/10 bg-white/[0.045] p-5 backdrop-blur">
              <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">
                Archive Total
              </p>
              <p className="mt-4 text-3xl font-semibold text-white">
                {loading ? "..." : total}
              </p>
              <p className="mt-2 text-sm text-slate-400">
                Count reported by the backend memories endpoint
              </p>
            </article>

            <article className="rounded-[1.6rem] border border-cyan-400/15 bg-cyan-400/[0.07] p-5">
              <p className="text-xs font-semibold uppercase tracking-[0.24em] text-cyan-200/80">
                Viewer
              </p>
              <p className="mt-4 text-sm leading-7 text-cyan-50/90">
                Full-screen lightbox supports both stills and video clips from
                the same API payload.
              </p>
            </article>
          </div>
        </div>
      </section>

      {error ? (
        <div className="rounded-2xl border border-rose-400/20 bg-rose-400/10 px-4 py-3 text-sm text-rose-100">
          {error}
        </div>
      ) : null}

      <section className="rounded-[2rem] border border-white/10 bg-slate-950/55 p-4 shadow-xl shadow-black/20 sm:p-5">
        {loading ? (
          <div className="grid grid-cols-2 gap-4 md:grid-cols-4 lg:grid-cols-6">
            {Array.from({ length: 12 }, (_, index) => (
              <MemorySkeleton key={index} />
            ))}
          </div>
        ) : memories.length === 0 ? (
          <div className="flex min-h-[360px] flex-col items-center justify-center rounded-[1.5rem] border border-dashed border-white/10 bg-white/[0.025] px-6 text-center">
            <div className="rounded-2xl bg-cyan-400/10 p-4 text-cyan-200">
              <ImageIcon className="h-6 w-6" />
            </div>
            <h2 className="mt-5 text-2xl font-semibold text-white">
              No memories available yet
            </h2>
            <p className="mt-3 max-w-lg text-sm leading-7 text-slate-400">
              Import an archive on the dashboard first, then return here to
              browse the generated thumbnail grid.
            </p>
          </div>
        ) : (
          <>
            <div className="grid grid-cols-2 gap-4 md:grid-cols-4 lg:grid-cols-6">
              {memories.map((memory) => (
                <button
                  key={memory.asset_id}
                  type="button"
                  onClick={() => setSelectedMemory(memory)}
                  className="group relative aspect-[3/4] overflow-hidden rounded-[1.5rem] border border-white/10 bg-slate-900 text-left shadow-lg shadow-black/25 transition-transform duration-300 hover:-translate-y-1"
                >
                  {memory.thumbnail_url ? (
                    <img
                      src={memory.thumbnail_url}
                      alt={formatMemoryLabel(memory)}
                      loading="lazy"
                      className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-105"
                    />
                  ) : (
                    <div className="flex h-full items-center justify-center bg-white/[0.04] text-slate-500">
                      <ImageIcon className="h-8 w-8" />
                    </div>
                  )}

                  <div className="pointer-events-none absolute inset-x-0 top-0 flex items-start justify-between p-3">
                    <span className="rounded-full border border-white/10 bg-black/35 px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.2em] text-white/85 backdrop-blur">
                      {isVideo(memory) ? "Video" : "Memory"}
                    </span>
                    <div className="flex items-center gap-2">
                      {memory.is_favorite ? (
                        <span className="rounded-full bg-rose-400/20 p-2 text-rose-100 backdrop-blur">
                          <Heart className="h-3.5 w-3.5 fill-current" />
                        </span>
                      ) : null}
                      <span className="rounded-full bg-black/35 p-2 text-white/85 backdrop-blur">
                        <Expand className="h-3.5 w-3.5" />
                      </span>
                    </div>
                  </div>

                  <div className="pointer-events-none absolute inset-x-0 bottom-0 bg-gradient-to-t from-black/90 via-black/35 to-transparent px-4 pb-4 pt-10">
                    <p className="truncate text-sm font-medium text-white">
                      {formatMemoryLabel(memory)}
                    </p>
                    <p className="mt-1 text-xs uppercase tracking-[0.18em] text-slate-300/90">
                      {memory.year || "Archive"}
                    </p>
                  </div>
                </button>
              ))}
            </div>

            {hasMore ? (
              <div className="mt-6 flex justify-center">
                <button
                  type="button"
                  onClick={() => void loadMore()}
                  disabled={loadingMore}
                  className="inline-flex items-center gap-2 rounded-2xl border border-white/10 bg-white/[0.045] px-5 py-3 text-sm font-medium text-slate-100 transition hover:bg-white/[0.09] disabled:cursor-not-allowed disabled:opacity-70"
                >
                  {loadingMore ? (
                    <LoaderCircle className="h-4 w-4 animate-spin" />
                  ) : (
                    <ImageIcon className="h-4 w-4" />
                  )}
                  <span>{loadingMore ? "Loading more..." : "Load More"}</span>
                </button>
              </div>
            ) : null}
          </>
        )}
      </section>

      {selectedMemory ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/85 p-4 backdrop-blur-md"
          onClick={() => setSelectedMemory(null)}
        >
          <div
            className="relative flex max-h-[92vh] w-full max-w-6xl flex-col overflow-hidden rounded-[2rem] border border-white/10 bg-[#060c14] shadow-2xl shadow-black/50"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="flex items-center justify-between border-b border-white/10 px-5 py-4">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.25em] text-slate-500">
                  Media Viewer
                </p>
                <p className="mt-2 text-sm text-slate-200">
                  {formatMemoryLabel(selectedMemory)}
                </p>
              </div>

              <button
                type="button"
                onClick={() => setSelectedMemory(null)}
                className="rounded-2xl border border-white/10 bg-white/[0.05] p-3 text-slate-200 transition hover:bg-white/[0.1]"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="flex min-h-0 flex-1 items-center justify-center bg-[radial-gradient(circle_at_top,_rgba(24,38,59,0.45),_rgba(4,6,10,0.96)_60%)] p-4 sm:p-6">
              {selectedMemory.media_url ? (
                isVideo(selectedMemory) ? (
                  <div className="w-full">
                    <div className="mb-4 inline-flex items-center gap-2 rounded-full border border-cyan-400/20 bg-cyan-400/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-cyan-100">
                      <Clapperboard className="h-3.5 w-3.5" />
                      Video
                    </div>
                    <video
                      src={selectedMemory.media_url}
                      controls
                      autoPlay
                      className="max-h-[72vh] w-full rounded-[1.5rem] border border-white/10 bg-black object-contain"
                    />
                  </div>
                ) : (
                  <img
                    src={selectedMemory.media_url}
                    alt={formatMemoryLabel(selectedMemory)}
                    className="max-h-[78vh] w-full rounded-[1.5rem] border border-white/10 bg-black object-contain"
                  />
                )
              ) : (
                <div className="flex min-h-[320px] items-center justify-center rounded-[1.5rem] border border-white/10 bg-white/[0.03] px-6 text-center text-slate-400">
                  Full-resolution media is unavailable for this memory.
                </div>
              )}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
