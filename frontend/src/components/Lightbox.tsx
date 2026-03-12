import { Clapperboard, X } from "lucide-react";
import { useEffect, useState } from "react";

type LightboxProps = {
  mediaUrl: string | null;
  overlayUrl?: string | null;
  isVideo: boolean;
  title?: string;
  onClose: () => void;
};

export default function Lightbox({
  mediaUrl,
  overlayUrl,
  isVideo,
  title,
  onClose,
}: LightboxProps) {
  const [mediaFailed, setMediaFailed] = useState(false);
  const [overlayFailed, setOverlayFailed] = useState(false);

  useEffect(() => {
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", handleEscape);

    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", handleEscape);
    };
  }, [onClose]);

  useEffect(() => {
    setMediaFailed(false);
    setOverlayFailed(false);
  }, [mediaUrl, overlayUrl, isVideo]);

  return (
    <div
      className="fixed inset-0 z-50 bg-black/95 flex items-center justify-center p-4"
      onClick={onClose}
    >
      <div
        className="relative flex h-full max-h-[90vh] w-full max-w-[90vw] flex-col overflow-hidden rounded-[2rem] border border-white/10 bg-[#060c14] shadow-2xl shadow-black/50"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-white/10 px-5 py-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.25em] text-slate-500">
              Media Viewer
            </p>
            {title ? (
              <p className="mt-2 text-sm text-slate-200">{title}</p>
            ) : null}
          </div>

          <button
            type="button"
            onClick={onClose}
            className="rounded-2xl border border-white/10 bg-white/[0.05] p-3 text-slate-200 transition hover:bg-white/[0.1]"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="flex min-h-0 flex-1 items-center justify-center bg-[radial-gradient(circle_at_top,_rgba(24,38,59,0.45),_rgba(4,6,10,0.96)_60%)] p-4 sm:p-6">
          <div className="relative h-full max-h-[90vh] w-full max-w-[90vw] flex items-center justify-center">
            {isVideo ? (
              <div className="absolute left-0 top-0 z-10 inline-flex items-center gap-2 rounded-full border border-cyan-400/20 bg-cyan-400/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-cyan-100">
                <Clapperboard className="h-3.5 w-3.5" />
                Video
              </div>
            ) : null}

            {mediaUrl && !mediaFailed ? (
              <div className="relative w-full h-full max-h-[90vh] max-w-[90vw]">
                {isVideo ? (
                  <video
                    src={mediaUrl}
                    controls
                    playsInline
                    autoPlay
                    onError={() => setMediaFailed(true)}
                    className="absolute inset-0 w-full h-full object-contain"
                  />
                ) : (
                  <img
                    src={mediaUrl}
                    alt={title || "Media"}
                    onError={() => setMediaFailed(true)}
                    className="absolute inset-0 w-full h-full object-contain"
                  />
                )}

                {overlayUrl && !overlayFailed ? (
                  <img
                    src={overlayUrl}
                    alt=""
                    aria-hidden="true"
                    onError={() => setOverlayFailed(true)}
                    className="absolute inset-0 w-full h-full object-contain pointer-events-none"
                  />
                ) : null}
              </div>
            ) : (
              <div className="relative w-full h-full max-h-[90vh] max-w-[90vw] rounded-[1.5rem] border border-white/10 bg-white/[0.03] px-6 text-center text-slate-400 flex items-center justify-center">
                Full-resolution media is unavailable for this item.
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
