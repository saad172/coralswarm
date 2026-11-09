/* ============================================================
   CoralSwarm UI kit.

   Source of truth: the 2026-07 redesign handoff
   (New UI Design/design_handoff_coralswarm_redesign/), documented in
   docs/design/DESIGN_SYSTEM.md.

   This replaces the single-file components/ui.tsx. `@/components/ui` still
   resolves here (directory index), so no import in the app changed.

   THE RULE: screens compose these. No screen hardcodes a colour, radius,
   shadow or font stack — if a value isn't in the system, the system changes
   first. Every primitive takes `className` for layout-level overrides (margin,
   width, grid placement) and exposes props for anything visual.
   ============================================================ */

export { cn } from "./cn";
export { HUES, hueVar, hueTint, hueForName, workspaceHue, type Hue } from "./hue";

export { Button, buttonClasses, IconButton, SuggestionPill, type ButtonProps, type ButtonVariant, type ButtonSize } from "./Button";
export { Card, CardHead, Panel, Divider, type CardProps } from "./Card";
export { Eyebrow, Mono, PageTitle, PageHead, SectionHead, TitleAccent } from "./Text";
export { Badge, Chip, CountPill, Dot, ScopePill, Avatar, Verdict, type BadgeTone, type VerdictTone } from "./Badge";
export { FieldShell, Input, Textarea, Select, SearchInput, Toggle } from "./Field";
export { Checkbox } from "./Checkbox";
export { OptionTile } from "./OptionTile";
export { CopyPill } from "./CopyPill";
export { Sparkline } from "./Sparkline";
export { Seg, LevelDial, Tabs, FacetRow, NavRow, activeRow } from "./Nav";
export { StepRail, type StepState } from "./StepRail";
export { ListHead, ListHeadCell, ListRow, RowTitle, RowMeta, TableGrid, Th, Td } from "./Table";
export { Empty, Loading, ErrorNote, Notice, Skeleton, Meter, StackedMeter, type NoticeTone } from "./States";
export { Modal, Popover, MenuItem } from "./Modal";
export { DotMandala, Wordmark } from "./Mark";
export { SwarmStream } from "./SwarmStream";
export { CiteChip, SourceCard, AnswerMeta } from "./Cite";
