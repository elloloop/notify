// Single source of truth for the documentation nav. Used by:
//   - the sidebar (rendered in DocsLayout)
//   - breadcrumbs
//   - prev/next links at the bottom of every page
//
// Keep `href` paths in sync with the file paths under src/pages/.

export interface NavItem {
  label: string;
  href: string;
}

export interface NavSection {
  title: string;
  items: NavItem[];
}

export const BASE = "/notify";

export const sidebarSections: NavSection[] = [
  {
    title: "Getting Started",
    items: [
      { label: "Introduction", href: `${BASE}/` },
      { label: "What is notify", href: `${BASE}/docs/introduction` },
      { label: "Quick Start", href: `${BASE}/docs/quickstart` },
    ],
  },
  {
    title: "Concepts",
    items: [
      { label: "Architecture", href: `${BASE}/docs/concepts/architecture` },
      { label: "Channels & Providers", href: `${BASE}/docs/concepts/channels-and-providers` },
      { label: "Store & Conformance", href: `${BASE}/docs/concepts/store-and-conformance` },
      { label: "Realtime Engine", href: `${BASE}/docs/concepts/realtime-engine` },
      { label: "Auth Model", href: `${BASE}/docs/concepts/auth-model` },
      { label: "Multi-Tenancy", href: `${BASE}/docs/concepts/multi-tenancy` },
    ],
  },
  {
    title: "Installation",
    items: [
      { label: "Docker", href: `${BASE}/docs/installation/docker` },
      { label: "Configuration", href: `${BASE}/docs/installation/configuration` },
      { label: "JWT Keys", href: `${BASE}/docs/installation/jwt-keys` },
      { label: "Store Setup", href: `${BASE}/docs/installation/store-setup` },
    ],
  },
  {
    title: "Channels",
    items: [
      { label: "Email", href: `${BASE}/docs/channels/email` },
      { label: "SMS", href: `${BASE}/docs/channels/sms` },
      { label: "WhatsApp", href: `${BASE}/docs/channels/whatsapp` },
      { label: "Web Push", href: `${BASE}/docs/channels/web-push` },
      { label: "Mobile Push (FCM/APNs)", href: `${BASE}/docs/channels/mobile-push` },
      { label: "In-App (SSE)", href: `${BASE}/docs/channels/in-app` },
    ],
  },
  {
    title: "Store Drivers",
    items: [
      { label: "Memory", href: `${BASE}/docs/store/memory` },
      { label: "Postgres", href: `${BASE}/docs/store/postgres` },
      { label: "EntDB", href: `${BASE}/docs/store/entdb` },
      { label: "Conformance Suite", href: `${BASE}/docs/store/conformance` },
    ],
  },
  {
    title: "API Reference",
    items: [
      { label: "gRPC / Connect Services", href: `${BASE}/docs/api-reference/grpc` },
    ],
  },
  {
    title: "Operations",
    items: [
      { label: "Observability", href: `${BASE}/docs/operations/observability` },
      { label: "Graceful Shutdown", href: `${BASE}/docs/operations/shutdown` },
    ],
  },
  {
    title: "Deployment",
    items: [
      { label: "Docker", href: `${BASE}/docs/deployment/docker` },
      { label: "Kubernetes", href: `${BASE}/docs/deployment/kubernetes` },
      { label: "GitHub Actions", href: `${BASE}/docs/deployment/github-actions` },
    ],
  },
  {
    title: "Examples",
    items: [
      { label: "Send a Notification", href: `${BASE}/docs/examples/send-notification` },
      { label: "Subscribe over SSE", href: `${BASE}/docs/examples/subscribe-sse` },
      { label: "Register a Push Token", href: `${BASE}/docs/examples/register-push-token` },
      { label: "Ack a Notification", href: `${BASE}/docs/examples/ack-notification` },
      { label: "Multi-Channel Fanout", href: `${BASE}/docs/examples/multi-channel-fanout` },
    ],
  },
];

// Flat ordered list with section labels — drives breadcrumbs and prev/next.
export interface FlatNavItem extends NavItem {
  section: string;
}

export const flatNav: FlatNavItem[] = sidebarSections.flatMap((section) =>
  section.items.map((item) => ({ ...item, section: section.title })),
);

// Normalize a path to match how `href` is defined above (strip trailing slash,
// keep root as just the BASE).
function normalize(p: string): string {
  if (!p) return p;
  if (p === BASE || p === `${BASE}/`) return `${BASE}/`;
  return p.replace(/\/+$/, "");
}

export function findCurrent(currentPath: string): FlatNavItem | undefined {
  const target = normalize(currentPath);
  return flatNav.find(
    (item) => normalize(item.href) === target || item.href === target,
  );
}

export function findPrevNext(currentPath: string): {
  prev?: FlatNavItem;
  next?: FlatNavItem;
} {
  const target = normalize(currentPath);
  const idx = flatNav.findIndex(
    (item) => normalize(item.href) === target || item.href === target,
  );
  if (idx === -1) return {};
  return {
    prev: idx > 0 ? flatNav[idx - 1] : undefined,
    next: idx < flatNav.length - 1 ? flatNav[idx + 1] : undefined,
  };
}

export function buildBreadcrumbs(
  currentPath: string,
): { label: string; href?: string }[] {
  const current = findCurrent(currentPath);
  const docsRoot = { label: "Docs", href: `${BASE}/` };
  if (!current) return [docsRoot];
  // Root introduction page — just "Docs".
  if (current.href === `${BASE}/`) return [docsRoot];
  return [
    docsRoot,
    { label: current.section },
    { label: current.label },
  ];
}
