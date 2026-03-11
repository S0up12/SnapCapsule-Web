import { MessageSquareText } from "lucide-react";

export default function ChatsView() {
  return (
    <section className="mx-auto max-w-6xl rounded-[2rem] border border-white/10 bg-slate-950/70 p-8 shadow-lg shadow-black/20">
      <div className="flex items-start gap-4">
        <div className="rounded-2xl bg-cyan-400/10 p-3 text-cyan-200">
          <MessageSquareText className="h-5 w-5" />
        </div>
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.25em] text-slate-500">
            Chats
          </p>
          <h1 className="mt-3 text-3xl font-semibold text-white">
            Chat explorer is queued next.
          </h1>
          <p className="mt-4 max-w-2xl text-sm leading-7 text-slate-300">
            Routing and layout are already wired, so this view can stay lightweight
            until the real conversation list and message pane are implemented.
          </p>
        </div>
      </div>
    </section>
  );
}
