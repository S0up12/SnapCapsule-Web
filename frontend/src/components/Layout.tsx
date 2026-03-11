import { Menu, MessageSquareText, PanelLeftClose, PanelsTopLeft, Sparkles } from "lucide-react";
import { useState } from "react";
import { NavLink, Outlet } from "react-router-dom";

const navItems = [
  {
    to: "/",
    label: "Dashboard",
    icon: PanelsTopLeft,
  },
  {
    to: "/memories",
    label: "Memories",
    icon: Sparkles,
  },
  {
    to: "/chats",
    label: "Chats",
    icon: MessageSquareText,
  },
];

function NavItems({ onNavigate }: { onNavigate?: () => void }) {
  return (
    <nav className="space-y-2">
      {navItems.map((item) => {
        const Icon = item.icon;
        return (
          <NavLink
            key={item.label}
            to={item.to}
            end={item.to === "/"}
            onClick={onNavigate}
            className={({ isActive }) =>
              [
                "flex items-center gap-3 rounded-2xl px-4 py-3 text-sm font-medium transition",
                isActive
                  ? "bg-cyan-400/15 text-white shadow-glow"
                  : "text-slate-400 hover:bg-white/5 hover:text-slate-100",
              ].join(" ")
            }
          >
            <Icon className="h-4 w-4" />
            <span>{item.label}</span>
          </NavLink>
        );
      })}
    </nav>
  );
}

export default function Layout() {
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top_left,_#13253b,_#091019_42%,_#05080e_100%)] text-slate-100">
      <div className="flex min-h-screen">
        <aside className="hidden w-72 shrink-0 border-r border-white/10 bg-slate-950/65 p-6 backdrop-blur xl:block">
          <div className="flex h-full flex-col">
            <div className="flex items-center gap-3 rounded-2xl border border-white/10 bg-white/5 px-4 py-4">
              <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-cyan-400/15 text-cyan-200">
                <PanelLeftClose className="h-5 w-5" />
              </div>
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.28em] text-cyan-300/70">
                  SnapCapsule
                </p>
                <p className="text-sm text-slate-300">Self-hosted archive</p>
              </div>
            </div>

            <div className="mt-8">
              <p className="mb-3 px-2 text-xs font-semibold uppercase tracking-[0.25em] text-slate-500">
                Navigation
              </p>
              <NavItems />
            </div>

            <div className="mt-auto rounded-[1.5rem] border border-white/10 bg-white/5 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.25em] text-slate-500">
                Deployment
              </p>
              <p className="mt-3 text-sm leading-6 text-slate-300">
                Optimized for the self-hosted stack: FastAPI backend, React UI,
                and Docker-first deployment.
              </p>
            </div>
          </div>
        </aside>

        <div className="flex min-h-screen min-w-0 flex-1 flex-col">
          <header className="sticky top-0 z-20 border-b border-white/10 bg-slate-950/45 px-5 py-4 backdrop-blur xl:hidden">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.28em] text-cyan-300/70">
                  SnapCapsule
                </p>
                <p className="text-sm text-slate-400">Archive dashboard</p>
              </div>
              <button
                type="button"
                onClick={() => setMobileOpen((value) => !value)}
                className="rounded-2xl border border-white/10 bg-white/5 p-3 text-slate-200 transition hover:bg-white/10"
              >
                <Menu className="h-5 w-5" />
              </button>
            </div>
          </header>

          {mobileOpen ? (
            <div className="border-b border-white/10 bg-slate-950/95 px-5 py-5 xl:hidden">
              <NavItems onNavigate={() => setMobileOpen(false)} />
            </div>
          ) : null}

          <main className="flex-1 px-5 py-6 md:px-8 md:py-8">
            <Outlet />
          </main>
        </div>
      </div>
    </div>
  );
}
