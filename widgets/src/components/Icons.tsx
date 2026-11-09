// Small inline icon set, paths copied verbatim from the approved artboards
// (widget-design/*.dc.html) rather than pulled from an icon-font dependency —
// keeps the bundle self-contained and pixel-identical to the design.

import type { JSX } from "preact";

interface IconProps {
  size?: number;
  color: string;
  style?: JSX.CSSProperties;
}

function base(size: number) {
  return {
    viewBox: "0 0 24 24",
    width: size,
    height: size,
    fill: "none",
    strokeWidth: 1.8,
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
  };
}

export function ExternalLinkIcon({ size = 12, color, style }: IconProps) {
  return (
    <svg {...base(size)} stroke={color} style={{ flex: "none", display: "block", ...style }}>
      <path d="M14 4h6v6M20 4l-9 9" />
      <path d="M18 13v7H4V6h7" />
    </svg>
  );
}

export function CloseIcon({ size = 13, color, style }: IconProps) {
  return (
    <svg {...base(size)} stroke={color} style={{ flex: "none", display: "block", ...style }}>
      <path d="M6 6l12 12M18 6 6 18" />
    </svg>
  );
}

export function ChevronDownIcon({ size = 12, color, style }: IconProps) {
  return (
    <svg {...base(size)} stroke={color} style={{ flex: "none", display: "block", ...style }}>
      <path d="m6 9 6 6 6-6" />
    </svg>
  );
}

export function MeetingIcon({ size = 14, color, style }: IconProps) {
  return (
    <svg {...base(size)} stroke={color} style={{ flex: "none", display: "block", ...style }}>
      <path d="M3 5h13v14H3z" />
      <path d="m16 10 5-3v10l-5-3z" />
    </svg>
  );
}

export function SlackIcon({ size = 14, color, style }: IconProps) {
  return (
    <svg {...base(size)} stroke={color} style={{ flex: "none", display: "block", ...style }}>
      <path d="M9 3v9M9 3a2 2 0 1 0 0 4M15 21v-9M15 21a2 2 0 1 0 0-4M3 15h9M3 15a2 2 0 1 0 4 0M21 9h-9M21 9a2 2 0 1 0-4 0" />
    </svg>
  );
}

export function CodingIcon({ size = 14, color, style }: IconProps) {
  return (
    <svg {...base(size)} stroke={color} style={{ flex: "none", display: "block", ...style }}>
      <path d="m7 8-4 4 4 4M17 8l4 4-4 4M14 4l-4 16" />
    </svg>
  );
}

export function DocIcon({ size = 14, color, style }: IconProps) {
  return (
    <svg {...base(size)} stroke={color} style={{ flex: "none", display: "block", ...style }}>
      <path d="M5 4h14v12l-5 5H5z" />
      <path d="M14 21v-5h5M10 13h6M10 17h6" />
    </svg>
  );
}

export function EmailIcon({ size = 14, color, style }: IconProps) {
  return (
    <svg {...base(size)} stroke={color} style={{ flex: "none", display: "block", ...style }}>
      <path d="M3 5h18v14H3z" />
      <path d="m3 7 9 6 9-6" />
    </svg>
  );
}

export function NoteIcon({ size = 14, color, style }: IconProps) {
  return (
    <svg {...base(size)} stroke={color} style={{ flex: "none", display: "block", ...style }}>
      <path d="M9 3v9M9 3a2 2 0 1 0 0 4M15 21v-9M15 21a2 2 0 1 0 0-4M3 15h9M3 15a2 2 0 1 0 4 0M21 9h-9M21 9a2 2 0 1 0-4 0" />
    </svg>
  );
}

/** Fallback glyph for a source_type none of the artboards drew an icon for
 *  (mcp, coding_session's coding-session icon is CodingIcon above; this one
 *  covers e.g. gdrive/teams_*). A plain circle keeps the row's icon column
 *  aligned rather than collapsing it. */
export function GenericSourceIcon({ size = 14, color, style }: IconProps) {
  return (
    <svg {...base(size)} stroke={color} style={{ flex: "none", display: "block", ...style }}>
      <circle cx="12" cy="12" r="8" />
    </svg>
  );
}

export function iconForSourceType(sourceType: string | undefined | null) {
  switch (sourceType) {
    case "meeting":
    case "transcript":
    case "zoom":
    case "meet":
    case "teams_transcript":
      return MeetingIcon;
    case "slack":
    case "chat":
    case "teams_chat":
    case "teams_channel":
      return SlackIcon;
    case "coding_session":
      return CodingIcon;
    case "email":
      return EmailIcon;
    case "note":
      return NoteIcon;
    case "document":
    case "notion":
    case "gdrive":
      return DocIcon;
    default:
      return GenericSourceIcon;
  }
}
