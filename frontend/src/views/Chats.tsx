import {
  ChevronLeft,
  ImageIcon,
  LoaderCircle,
  MessageSquareText,
  Search,
  Video,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import Lightbox from "../components/Lightbox";

type ConversationApiItem = {
  username: string;
  display_name: string | null;
  message_count: number | null;
};

type ConversationsResponse = {
  items: ConversationApiItem[];
  total: number;
};

type Conversation = ConversationApiItem;

type ChatMedia = {
  media_url: string | null;
  thumbnail_url: string | null;
  overlay_url: string | null;
};

type SelectedMedia = {
  mediaUrl: string | null;
  overlayUrl: string | null;
  isVideo: boolean;
  title: string;
};

type ChatMessage = {
  id: number;
  sender: string;
  content: string;
  timestamp: string;
  msg_type: string;
  source: string;
  media: ChatMedia[];
};

type MessagesResponse = {
  account_id: string;
  items: ChatMessage[];
  skip: number;
  limit: number;
  total: number;
};

const PAGE_SIZE = 50;

function formatTime(timestamp: string) {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return timestamp;
  }

  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function formatDay(timestamp: string) {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return "Unknown date";
  }

  return new Intl.DateTimeFormat(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(date);
}

function formatCount(value: number | null) {
  if (value === null) {
    return "...";
  }

  return new Intl.NumberFormat().format(value);
}

function isVideoMedia(url: string | null) {
  if (!url) {
    return false;
  }

  return /\.(mp4|mov|avi|webm|m4v)(\?|$)/i.test(url);
}

function conversationLabel(conversation: Conversation | null) {
  if (!conversation) {
    return "";
  }

  return conversation.display_name || conversation.username;
}

function ChatMediaPlaceholder({ video }: { video: boolean }) {
  return (
    <div className="flex min-h-[180px] items-center justify-center bg-gray-800 px-4 text-center text-slate-400 animate-pulse">
      <div className="flex flex-col items-center gap-2">
        {video ? <Video className="h-8 w-8" /> : <ImageIcon className="h-8 w-8" />}
        <span className="text-xs font-medium uppercase tracking-[0.18em]">
          {video ? "Video unavailable" : "Preview unavailable"}
        </span>
      </div>
    </div>
  );
}

type ChatMediaPreviewProps = {
  media: ChatMedia;
  messageTitle: string;
  onOpen: (selection: SelectedMedia) => void;
};

function ChatMediaPreview({ media, messageTitle, onOpen }: ChatMediaPreviewProps) {
  const video = isVideoMedia(media.media_url);
  const previewUrl = media.thumbnail_url || (!video ? media.media_url : null);
  const mediaUrl = media.media_url;
  const [failed, setFailed] = useState(false);

  if (!previewUrl || failed) {
    return <ChatMediaPlaceholder video={video} />;
  }

  if (video) {
    return (
      <button
        type="button"
        onClick={(event) => {
          event.preventDefault();
          onOpen({
            mediaUrl,
            overlayUrl: media.overlay_url,
            isVideo: true,
            title: messageTitle,
          });
        }}
        className="relative block w-full"
      >
        <img
          src={previewUrl}
          alt={messageTitle}
          loading="lazy"
          onError={() => setFailed(true)}
          className="max-h-[360px] w-full cursor-pointer bg-black object-cover transition-opacity hover:opacity-90"
        />
        <div className="pointer-events-none absolute inset-0 flex items-center justify-center bg-black/20">
          <div className="rounded-full border border-white/15 bg-black/45 p-3 text-white shadow-lg shadow-black/40">
            <Video className="h-5 w-5" />
          </div>
        </div>
      </button>
    );
  }

  return (
    <img
      src={previewUrl}
      alt={messageTitle}
      loading="lazy"
      onError={() => setFailed(true)}
      onClick={() =>
        onOpen({
          mediaUrl: mediaUrl || previewUrl,
          overlayUrl: media.overlay_url,
          isVideo: false,
          title: messageTitle,
        })
      }
      className="max-h-[420px] w-full cursor-pointer object-cover transition-opacity hover:opacity-90"
    />
  );
}

export default function Chats() {
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [selectedAccountId, setSelectedAccountId] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [totalMessages, setTotalMessages] = useState(0);
  const [sidebarLoading, setSidebarLoading] = useState(true);
  const [messageLoading, setMessageLoading] = useState(false);
  const [loadingOlder, setLoadingOlder] = useState(false);
  const [sidebarError, setSidebarError] = useState<string | null>(null);
  const [messageError, setMessageError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [showSidebarOnMobile, setShowSidebarOnMobile] = useState(true);
  const [selectedMedia, setSelectedMedia] = useState<SelectedMedia | null>(null);
  const messagesViewportRef = useRef<HTMLDivElement | null>(null);

  const selectedConversation = useMemo(
    () =>
      conversations.find((conversation) => conversation.username === selectedAccountId) ??
      null,
    [conversations, selectedAccountId],
  );

  const filteredConversations = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!query) {
      return conversations;
    }

    return conversations.filter((conversation) => {
      const label = conversationLabel(conversation).toLowerCase();
      return (
        label.includes(query) ||
        conversation.username.toLowerCase().includes(query)
      );
    });
  }, [conversations, search]);

  const hasOlderMessages = messages.length < totalMessages;

  useEffect(() => {
    void fetchConversations();
  }, []);

  useEffect(() => {
    if (!selectedAccountId) {
      return;
    }

    void fetchMessages(selectedAccountId, 0, false);
  }, [selectedAccountId]);

  async function fetchConversations() {
    try {
      setSidebarLoading(true);
      const response = await fetch("/api/chats/");
      if (!response.ok) {
        throw new Error(`Conversations request failed with ${response.status}`);
      }

      const payload = (await response.json()) as ConversationsResponse;
      setConversations(payload.items);
      setSidebarError(null);
    } catch (requestError) {
      setSidebarError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to load conversations.",
      );
    } finally {
      setSidebarLoading(false);
    }
  }

  async function fetchMessages(accountId: string, skip: number, appendOlder: boolean) {
    const viewport = messagesViewportRef.current;
    const previousHeight = viewport?.scrollHeight ?? 0;
    const previousTop = viewport?.scrollTop ?? 0;

    try {
      if (appendOlder) {
        setLoadingOlder(true);
      } else {
        setMessageLoading(true);
      }

      const response = await fetch(
        `/api/chats/${encodeURIComponent(accountId)}/messages?skip=${skip}&limit=${PAGE_SIZE}`,
      );
      if (!response.ok) {
        throw new Error(`Messages request failed with ${response.status}`);
      }

      const payload = (await response.json()) as MessagesResponse;
      const nextBatch = [...payload.items].reverse();

      setMessages((current) => (appendOlder ? [...nextBatch, ...current] : nextBatch));
      setTotalMessages(payload.total);
      setMessageError(null);
      setShowSidebarOnMobile(false);

      requestAnimationFrame(() => {
        const nextViewport = messagesViewportRef.current;
        if (!nextViewport) {
          return;
        }

        if (appendOlder) {
          const nextHeight = nextViewport.scrollHeight;
          nextViewport.scrollTop = nextHeight - previousHeight + previousTop;
        } else {
          nextViewport.scrollTop = nextViewport.scrollHeight;
        }
      });
    } catch (requestError) {
      setMessageError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to load messages.",
      );
    } finally {
      setMessageLoading(false);
      setLoadingOlder(false);
    }
  }

  async function loadOlderMessages() {
    if (!selectedAccountId || loadingOlder || messageLoading || !hasOlderMessages) {
      return;
    }

    await fetchMessages(selectedAccountId, messages.length, true);
  }

  function isOutgoingMessage(message: ChatMessage) {
    if (!selectedConversation) {
      return false;
    }

    const sender = (message.sender || "").trim().toLowerCase();
    const conversationId = selectedConversation.username.trim().toLowerCase();
    const displayName = (selectedConversation.display_name || "").trim().toLowerCase();

    return sender !== conversationId && sender !== displayName;
  }

  return (
    <div className="mx-auto flex h-[calc(100vh-7.5rem)] w-full max-w-[1600px] min-h-[720px] overflow-hidden rounded-[2rem] border border-white/10 bg-[linear-gradient(180deg,_rgba(8,14,24,0.96),_rgba(5,9,16,0.98))] shadow-2xl shadow-black/35">
      <aside
        className={[
          "z-10 w-full max-w-full shrink-0 border-r border-white/10 bg-slate-950/65 backdrop-blur lg:flex lg:w-[360px] lg:max-w-[360px] lg:flex-col",
          showSidebarOnMobile ? "flex flex-col" : "hidden",
        ].join(" ")}
      >
        <div className="border-b border-white/10 px-5 py-5">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-cyan-300/70">
            Chats
          </p>
          <h1 className="mt-3 text-2xl font-semibold text-white">
            Conversation Index
          </h1>
          <div className="mt-4 flex items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3">
            <Search className="h-4 w-4 text-slate-500" />
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search conversations"
              className="w-full bg-transparent text-sm text-slate-100 outline-none placeholder:text-slate-500"
            />
          </div>
        </div>

        {sidebarError ? (
          <div className="mx-5 mt-5 rounded-2xl border border-rose-400/20 bg-rose-400/10 px-4 py-3 text-sm text-rose-100">
            {sidebarError}
          </div>
        ) : null}

        <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3">
          {sidebarLoading ? (
            <div className="space-y-3">
              {Array.from({ length: 8 }, (_, index) => (
                <div
                  key={index}
                  className="h-24 animate-pulse rounded-[1.4rem] border border-white/10 bg-white/[0.035]"
                />
              ))}
            </div>
          ) : filteredConversations.length === 0 ? (
            <div className="flex h-full min-h-[280px] flex-col items-center justify-center px-6 text-center">
              <div className="rounded-2xl bg-cyan-400/10 p-4 text-cyan-200">
                <MessageSquareText className="h-6 w-6" />
              </div>
              <p className="mt-5 text-lg font-semibold text-white">
                No conversations found
              </p>
              <p className="mt-2 text-sm leading-7 text-slate-400">
                Adjust the search or import archive chat data first.
              </p>
            </div>
          ) : (
            <div className="space-y-2">
              {filteredConversations.map((conversation) => {
                const active = conversation.username === selectedAccountId;

                return (
                  <button
                    key={conversation.username}
                    type="button"
                    onClick={() => {
                      setSelectedAccountId(conversation.username);
                      setMessages([]);
                      setMessageError(null);
                    }}
                    className={[
                      "w-full rounded-[1.5rem] border px-4 py-4 text-left transition",
                      active
                        ? "border-cyan-400/20 bg-cyan-400/10 shadow-[0_10px_30px_rgba(34,211,238,0.08)]"
                        : "border-white/10 bg-white/[0.025] hover:border-white/15 hover:bg-white/[0.05]",
                    ].join(" ")}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="truncate text-sm font-semibold text-white">
                          {conversation.display_name || conversation.username}
                        </p>
                        <p className="mt-1 truncate text-xs uppercase tracking-[0.18em] text-slate-500">
                          {conversation.username}
                        </p>
                      </div>
                      <span className="rounded-full border border-white/10 bg-black/20 px-2.5 py-1 text-xs font-medium text-slate-300">
                        {formatCount(conversation.message_count)}
                      </span>
                    </div>

                    <div className="mt-4 flex items-center gap-2 text-xs text-slate-400">
                      <MessageSquareText className="h-3.5 w-3.5" />
                      <span>
                        {conversation.message_count === 1 ? "1 message" : `${formatCount(conversation.message_count)} messages`}
                      </span>
                    </div>
                  </button>
                );
              })}
            </div>
          )}
        </div>
      </aside>

      <section className="flex min-w-0 flex-1 flex-col">
        <header className="flex items-center gap-3 border-b border-white/10 bg-slate-950/35 px-4 py-4 sm:px-6">
          <button
            type="button"
            onClick={() => setShowSidebarOnMobile(true)}
            className="rounded-2xl border border-white/10 bg-white/[0.05] p-3 text-slate-200 transition hover:bg-white/[0.1] lg:hidden"
          >
            <ChevronLeft className="h-4 w-4" />
          </button>

          {selectedConversation ? (
            <div className="min-w-0">
              <p className="truncate text-lg font-semibold text-white">
                {selectedConversation.display_name || selectedConversation.username}
              </p>
              <p className="truncate text-xs uppercase tracking-[0.2em] text-slate-500">
                {selectedConversation.username}
              </p>
            </div>
          ) : (
            <div>
              <p className="text-lg font-semibold text-white">Chat Reader</p>
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500">
                Select a conversation
              </p>
            </div>
          )}
        </header>

        {selectedConversation ? (
          <>
            {messageError ? (
              <div className="mx-4 mt-4 rounded-2xl border border-rose-400/20 bg-rose-400/10 px-4 py-3 text-sm text-rose-100 sm:mx-6">
                {messageError}
              </div>
            ) : null}

            <div
              ref={messagesViewportRef}
              className="min-h-0 flex-1 overflow-y-auto bg-[radial-gradient(circle_at_top,_rgba(20,34,54,0.38),_rgba(6,10,18,0.97)_55%)] px-4 py-5 sm:px-6"
            >
              <div className="mx-auto flex max-w-4xl flex-col gap-5">
                {hasOlderMessages ? (
                  <div className="flex justify-center">
                    <button
                      type="button"
                      onClick={() => void loadOlderMessages()}
                      disabled={loadingOlder}
                      className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.05] px-4 py-2 text-sm font-medium text-slate-200 transition hover:bg-white/[0.09] disabled:cursor-not-allowed disabled:opacity-70"
                    >
                      {loadingOlder ? (
                        <LoaderCircle className="h-4 w-4 animate-spin" />
                      ) : null}
                      <span>{loadingOlder ? "Loading..." : "Load Older Messages"}</span>
                    </button>
                  </div>
                ) : null}

                {messageLoading && messages.length === 0 ? (
                  <div className="space-y-4">
                    {Array.from({ length: 8 }, (_, index) => (
                      <div
                        key={index}
                        className={[
                          "max-w-[78%] animate-pulse rounded-[1.4rem] border border-white/10 px-4 py-4",
                          index % 2 === 0
                            ? "mr-auto bg-white/[0.04]"
                            : "ml-auto bg-cyan-400/10",
                        ].join(" ")}
                      >
                        <div className="h-4 rounded bg-white/10" />
                      </div>
                    ))}
                  </div>
                ) : messages.length === 0 ? (
                  <div className="flex min-h-[420px] flex-col items-center justify-center rounded-[1.8rem] border border-dashed border-white/10 bg-white/[0.02] px-6 text-center">
                    <div className="rounded-2xl bg-cyan-400/10 p-4 text-cyan-200">
                      <MessageSquareText className="h-6 w-6" />
                    </div>
                    <p className="mt-5 text-xl font-semibold text-white">
                      No messages loaded
                    </p>
                    <p className="mt-2 max-w-md text-sm leading-7 text-slate-400">
                      This conversation exists in the archive, but the current
                      page returned no messages.
                    </p>
                  </div>
                ) : (
                  messages.map((message, index) => {
                    const outgoing = isOutgoingMessage(message);
                    const previousMessage = messages[index - 1];
                    const showDayDivider =
                      !previousMessage ||
                      formatDay(previousMessage.timestamp) !== formatDay(message.timestamp);

                    return (
                      <div key={message.id} className="space-y-3">
                        {showDayDivider ? (
                          <div className="flex justify-center">
                            <span className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-xs font-medium uppercase tracking-[0.18em] text-slate-400">
                              {formatDay(message.timestamp)}
                            </span>
                          </div>
                        ) : null}

                        <div className={outgoing ? "flex justify-end" : "flex justify-start"}>
                          <article
                            className={[
                              "max-w-[84%] rounded-[1.5rem] border px-4 py-3 shadow-lg shadow-black/20 sm:max-w-[75%]",
                              outgoing
                                ? "border-cyan-400/20 bg-[linear-gradient(145deg,_rgba(34,211,238,0.18),_rgba(12,90,122,0.2))] text-cyan-50"
                                : "border-white/10 bg-white/[0.05] text-slate-100",
                            ].join(" ")}
                          >
                            <div className="flex items-center gap-2">
                              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                                {outgoing ? "You" : message.sender}
                              </p>
                              <span className="text-[11px] text-slate-500">
                                {formatTime(message.timestamp)}
                              </span>
                            </div>

                            {message.content ? (
                              <p className="mt-3 whitespace-pre-wrap text-sm leading-7">
                                {message.content}
                              </p>
                            ) : null}

                            {message.media.length > 0 ? (
                              <div className="mt-3 grid gap-3">
                                {message.media.map((media, mediaIndex) => {
                                  return (
                                    <div
                                      key={`${message.id}-${mediaIndex}`}
                                      className="overflow-hidden rounded-[1.2rem] border border-white/10 bg-black/20"
                                    >
                                      <ChatMediaPreview
                                        media={media}
                                        messageTitle={message.content || "Chat media"}
                                        onOpen={setSelectedMedia}
                                      />
                                    </div>
                                  );
                                })}
                              </div>
                            ) : null}
                          </article>
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
            </div>

            <footer className="border-t border-white/10 bg-slate-950/35 px-4 py-4 sm:px-6">
              <div className="mx-auto flex max-w-4xl items-center justify-between gap-4 rounded-[1.5rem] border border-white/10 bg-white/[0.04] px-4 py-3">
                <div className="min-w-0">
                  <p className="truncate text-sm font-medium text-white">
                    {conversationLabel(selectedConversation)}
                  </p>
                  <p className="truncate text-xs uppercase tracking-[0.18em] text-slate-500">
                    {formatCount(totalMessages)} archived messages
                  </p>
                </div>

                <div className="flex items-center gap-2 text-slate-400">
                  <ImageIcon className="h-4 w-4" />
                  <Video className="h-4 w-4" />
                </div>
              </div>
            </footer>
          </>
        ) : (
          <div className="flex flex-1 items-center justify-center bg-[radial-gradient(circle_at_top,_rgba(18,31,49,0.36),_rgba(5,8,15,0.98)_60%)] px-6">
            <div className="max-w-lg text-center">
              <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-[1.5rem] border border-cyan-400/15 bg-cyan-400/10 text-cyan-200">
                <MessageSquareText className="h-7 w-7" />
              </div>
              <h2 className="mt-6 text-3xl font-semibold tracking-tight text-white">
                Select a conversation to start reading
              </h2>
              <p className="mt-4 text-sm leading-7 text-slate-400">
                The sidebar keeps the archive index on the left while the main
                pane becomes a focused message reader for the active thread.
              </p>
            </div>
          </div>
        )}
      </section>

      {selectedMedia ? (
        <Lightbox
          mediaUrl={selectedMedia.mediaUrl}
          overlayUrl={selectedMedia.overlayUrl}
          isVideo={selectedMedia.isVideo}
          title={selectedMedia.title}
          onClose={() => setSelectedMedia(null)}
        />
      ) : null}
    </div>
  );
}
