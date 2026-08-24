---
name: ABT Control Plane
description: A high-information financial operations console for supervising cross-platform hedging.
colors:
  primary: "#5b35c9"
  surface: "#ffffff"
  canvas: "#f8f8fb"
  ink: "#1c1a25"
  muted-ink: "#504e5d"
  rule: "#d8d8e2"
  danger: "#b42318"
  warning: "#9a6700"
  success: "#087443"
  info: "#2952a3"
typography:
  body:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: 1.5
  heading:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "clamp(1.375rem, 2vw, 1.75rem)"
    fontWeight: 700
    lineHeight: 1.15
    letterSpacing: "-0.035em"
  label:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.6875rem"
    fontWeight: 750
    lineHeight: 1.2
    letterSpacing: "0.07em"
rounded:
  control: "0.25rem"
  field: "0.4rem"
  card: "0.75rem"
  pill: "999px"
spacing:
  compact: "0.5rem"
  standard: "0.75rem"
  section: "1.25rem"
  page: "clamp(1rem, 2vw, 2rem)"
components:
  button-primary:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.surface}"
    rounded: "{rounded.field}"
    padding: "0.75rem 1rem"
  card:
    backgroundColor: "{colors.surface}"
    rounded: "{rounded.card}"
    padding: "2.5rem"
  status:
    rounded: "{rounded.pill}"
    padding: "0.125rem 0.5rem"
---

# Design System: ABT Control Plane

## Overview

**Creative North Star: "Signal Observatory"**

ABT is a high-information financial operations console. It presents multi-worker hedge activity as a calm, bounded field of signals: the operator sees the current state immediately, then deliberately opens the detail needed to act. The visual system is light, precise, and intentionally quiet so operational anomalies have room to speak.

The interface uses a restrained violet for authority and navigation, with semantic green, amber, red, and blue reserved for system state. Surfaces stay simple; meaningful hierarchy comes from alignment, compact type, rules, and progressive disclosure rather than decorative card stacks.

**Key Characteristics:**
- Quiet cool-paper canvas with crisp white working surfaces.
- Dense, tabular information designed for fast scanning and comparison.
- Violet is an action and navigation signal, never a background wash.
- State color is semantic and isolated to the smallest useful affordance.
- Details are revealed through rows, panels, and explicit actions rather than shown by default.

## Colors

The palette makes operator attention scarce: neutral layers carry the interface while violet denotes authority and semantic colors denote the health of the system.

### Primary
- **Command Violet:** used for primary actions, selected navigation, focus, and the few controls that change operational state.

### Neutral
- **Working Surface:** used for cards, tables, and the persistent sidebar.
- **Cool Canvas:** used behind operational surfaces to separate the application frame from work areas.
- **Operational Ink:** used for headings, values, and primary table content.
- **Muted Reading Ink:** used for supporting descriptions, secondary navigation, and labels.
- **Fine Rule:** used for table rows, input strokes, and structural separation.

### Named Rules
**The Violet Is Authority Rule.** Use violet for a selected destination, keyboard focus, or an action the operator can take; do not use it as generic decoration.

**The State Is Local Rule.** Green, amber, red, and blue communicate health only through status chips, terse messages, or the exact record they qualify.

## Typography

**Display Font:** Inter (with system sans-serif fallback)  
**Body Font:** Inter (with system sans-serif fallback)  
**Label Font:** Inter, compact uppercase labels with positive tracking

**Character:** Typography is utilitarian and compact without becoming cramped. Headings establish the operational destination; labels and numeric records support rapid scan paths.

### Hierarchy
- **Heading:** used for page and section titles; strong weight and tight tracking make a destination legible at a glance.
- **Body:** used for instructions and supporting state; keep contextual copy concise and bounded to readable measures.
- **Label:** used for column headings, statuses, and metadata; uppercase tracking separates metadata from values.

### Named Rules
**The Scan Before Read Rule.** Let alignment, tabular numerals, labels, and whitespace reveal the state before an operator needs to parse prose.

## Layout

The desktop shell pairs a 15rem persistent sidebar with a flexible working column. The main region uses a responsive page inset, a ruled toolbar, and compact sections separated by consistent vertical gaps. Tables can scroll horizontally rather than collapsing their record structure. On constrained viewports, preserve the information hierarchy and use explicit progressive disclosure instead of allowing long records to create uncontrolled surface stacks.

## Elevation & Depth

Depth is restrained and structural. Primary application regions are separated by cool canvas, white surfaces, and fine rules; only broad content cards receive the low ambient shadow `0 0.75rem 2rem rgb(30 20 70 / 12%)`. Tables and dense operational panels remain flat so the data grid, rather than elevation, carries hierarchy.

### Shadow Vocabulary
- **Ambient Card Lift:** `0 0.75rem 2rem rgb(30 20 70 / 12%)`; use only for isolated login and management containers.

### Named Rules
**The Flat Working Surface Rule.** Do not add shadows to rows, status chips, form fields, or nested panels. Use rules and spacing to describe structure.

## Shapes

Navigation controls use gently softened 0.25rem corners, inputs and buttons use 0.4rem corners, and broad containers use a 0.75rem radius. Statuses are compact pills. Borders are thin, neutral, and functional; rounded geometry should organize interaction targets rather than make the interface decorative.

## Components

### Buttons
- **Shape:** compact, gently rounded controls.
- **Primary:** Command Violet background with white text for a consequential operator action.
- **Secondary:** neutral outlined or text treatment for subordinate actions.
- **Focus:** a clear violet outline is mandatory for keyboard navigation.

### Cards / Containers
- **Corner Style:** broad container radius.
- **Background:** white working surface on a cool canvas.
- **Shadow Strategy:** isolated containers only; dense operational regions remain flat.
- **Internal Padding:** generous for standalone forms and management cards; compact for data panels.

### Inputs / Fields
- **Style:** white or canvas-adjacent fill, a Fine Rule stroke, and compact control radius.
- **Focus:** shift emphasis to Command Violet without adding a decorative glow.
- **Error:** use localized red text or status treatment next to the affected field.

### Navigation
- **Style:** quiet text links with a low-opacity violet selection surface.
- **Active State:** violet type plus a restrained violet tint.
- **Density:** compact, persistent, and grouped for efficient movement between operations.

### Tables
- **Style:** flat white grid with fine rules, compact 0.8125rem values, sticky uppercase headers, and tabular numerals.
- **Interaction:** a low-opacity violet row hover and a visible violet keyboard focus ring reveal a selectable record.
- **Disclosure:** open a record or action panel for full context; do not expand every secondary field in the base grid.

## Do's and Don'ts

### Do:
- **Do** make the current operational status scannable before exposing supporting detail.
- **Do** use explicit clicks, rows, and panels to reveal full records or risky actions.
- **Do** reserve semantic color for the state it names and keep it close to that state.
- **Do** preserve tabular alignment, sticky headers, and numeric legibility in dense data views.

### Don't:
- **Don't** turn every information group into a raised card.
- **Don't** use violet as a large decorative surface or as a substitute for information hierarchy.
- **Don't** expose every field by default when a concise summary and intentional disclosure is safer to scan.
- **Don't** use color alone to communicate operational health or actionability.
