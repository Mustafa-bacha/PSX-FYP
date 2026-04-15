# PSX Insight Frontend Design System (Stripe-inspired fintech)

This project uses a **fintech, dashboard-first visual language** inspired by Stripe-like clarity and data readability.

## 1) Visual Theme

- Tone: professional, trustworthy, calm
- Density: medium (data-rich but breathable)
- Surfaces: bright, soft elevation, subtle borders
- Motion: fast and minimal (hover lift + color transitions)

## 2) Color Tokens

- `--ad-bg`: `#f6f9fc`
- `--ad-surface`: `#ffffff`
- `--ad-surface-muted`: `#f8fafc`
- `--ad-border-color`: `#e6ebf1`
- `--ad-text-color`: `#0a2540`
- `--ad-text-color-secondary`: `#5b6b80`
- `--ad-primary-color`: `#635bff`
- `--ad-primary-hover`: `#4f46e5`
- `--ad-success`: `#16a34a`
- `--ad-danger`: `#dc2626`
- `--ad-warning`: `#d97706`

## 3) Typography

- Body: Inter/system sans
- Headings: Inter/system sans with semibold weights
- Monospace values/ticks: ui-monospace for compact numeric readability

## 4) Components

- Cards: 12px radius, subtle border, soft shadow
- Buttons:
  - default: neutral surface + border
  - primary: brand background + white text
  - danger: red background + white text
  - ghost: transparent with hover fill
- Inputs: 40px min height, clear focus ring, rounded 10px
- Alerts: semantic background + border + readable contrast

## 5) Layout Principles

- Sidebar/content split for desktop
- Responsive collapse to single column below `1024px`
- Vertical rhythm via 8/12/16/24 spacing scale
- Avoid full-page scrolling when panel-scrolling is available

## 6) Data Visualization

- Grid lines low-contrast
- Positive: green, negative: red
- Tooltips use surface + border styling and high contrast text

## 7) Do

- Use `ad-*` utility/component classes for new sections.
- Keep information hierarchy clear: title → context → actions.
- Keep buttons grouped by purpose and proximity.

## 8) Don’t

- Don’t use oversized glows or heavy gradients on data panels.
- Don’t mix unrelated accent colors in a single module.
- Don’t hide critical controls behind hover-only interactions.

## 9) Agent Prompt Guide

When editing UI in this repo, follow this instruction:

> Use the project `DESIGN.md` and `client/src/styles/awesome-design.css` design tokens and classes. Keep a fintech dashboard aesthetic (clean surfaces, subtle borders, `#635bff` primary, strong readability). Prefer `ad-card`, `ad-btn`, `ad-input`, `ad-grid`, and semantic text colors.
