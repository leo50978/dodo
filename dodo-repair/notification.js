export async function sendBroadcastNotification(payload = {}) {
  const title = payload.title || "Notification";
  const body = payload.body || "";
  const url = payload.url || "";

  if (typeof window === "undefined" || !("Notification" in window)) {
    return { ok: false, reason: "notifications-unsupported" };
  }

  if (Notification.permission !== "granted") {
    return { ok: false, reason: "permission-denied" };
  }

  const n = new Notification(title, {
    body,
    tag: payload.tag || "broadcast",
    icon: payload.icon || "./favicon.ico",
  });

  n.onclick = () => {
    if (url) window.location.href = url;
    window.focus();
    n.close();
  };

  return { ok: true };
}

export class NotificationComponent {
  constructor(options = {}) {
    this.mode = options.mode || "dashboard";
    this.defaultUrl = options.defaultUrl || "./";
    this.enabledStorageKey = options.enabledStorageKey || "dashboard_notifications_enabled";
  }

  init() {
    if (localStorage.getItem(this.enabledStorageKey) === null) {
      localStorage.setItem(this.enabledStorageKey, "false");
    }
  }

  isEnabled() {
    return localStorage.getItem(this.enabledStorageKey) === "true";
  }

  setEnabled(enabled) {
    localStorage.setItem(this.enabledStorageKey, enabled ? "true" : "false");
  }

  async requestPermission() {
    if (!("Notification" in window)) return "unsupported";
    if (Notification.permission === "granted") return "granted";
    return Notification.requestPermission();
  }

  notify(title, body, options = {}) {
    if (!this.isEnabled()) return;
    if (!("Notification" in window)) return;
    if (Notification.permission !== "granted") return;

    const n = new Notification(title || "Notification", {
      body: body || "",
      tag: options.tag || `${this.mode}_${Date.now()}`,
      icon: options.icon || "./favicon.ico",
    });

    n.onclick = () => {
      const targetUrl = options.url || this.defaultUrl;
      if (targetUrl) window.location.href = targetUrl;
      window.focus();
      n.close();
    };
  }
}
