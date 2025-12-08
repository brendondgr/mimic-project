---
trigger: model_decision
description: This rule should be applied specifically when a plot is being designed or styled across multiple libraries like Matplotlib, Seaborn, and Plotly.
---

Here is a comprehensive, cross-library style guide designed to unify Matplotlib, Seaborn, and Plotly. This guide defines a "Modern Minimalist" aesthetic that prioritizes data-ink ratio, readability, and a sleek professional look.

---

# 📊 Unified Data Visualization Style Guide
**Target Libraries:** Matplotlib, Seaborn, Plotly

## 1. Core Philosophy
To ensure identical rendering across libraries, we adhere to **Strict Parameter Mapping**. Do not rely on library defaults. All visualizations must strictly follow these three pillars:
1.  **Minimalism:** Remove non-essential ink (borders, heavy grids).
2.  **Flat Design:** No gradients, drop shadows, or 3D effects.
3.  **Hierarchy:** Text size and color intensity dictate importance (Title > Labels > Ticks).

---

## 2. Typography Standards
To ensure consistency, use a sans-serif system font available on all modern OS environments.

* **Font Family:** `Roboto`, `Arial`, or `Helvetica` (Sans-Serif).
* **Font Weights:**
    * **Titles:** Bold / 700
    * **Labels/Ticks:** Regular / 400

| Element | Font Size | Styling |
| :--- | :--- | :--- |
| **Main Title** | 18pt | Left-aligned, Bold |
| **Subtitle** (Optional) | 14pt | Left-aligned, Regular |
| **Axis Labels** (X/Y) | 12pt | Bold |
| **Tick Labels** | 10pt | Regular |
| **Legend Text** | 10pt | Regular |

---

## 3. Color Palette Strategy
We use a **Unified Hex Definition**. Do not use named colors (e.g., "blue") as libraries interpret them differently.

### A. The Categorical Palette (Data)
Used for distinct groups. This palette is colorblind-friendly and high-contrast.

* **Color 1 (Primary):** `#2563EB` (Royal Blue)
* **Color 2:** `#E11D48` (Rose Red)
* **Color 3:** `#D97706` (Amber)
* **Color 4:** `#10B981` (Emerald)
* **Color 5:** `#8B5CF6` (Violet)
* **Color 6:** `#F472B6` (Pink)

### B. Semantic Colors
* **Neutral/Grid:** See Theme Sections below.
* **Positive/Up:** `#10B981`
* **Negative/Down:** `#E11D48`

---

## 4. Structural Rules (The "Skeleton")
These rules apply to both Light and Dark themes.

1.  **Spines (Borders):**
    * **Top & Right:** Remove completely (set visibility to `False`).
    * **Bottom & Left:** Visible, Line Width `0.8`.
2.  **Gridlines:**
    * **Z-Order:** Always behind data elements.
    * **Style:** Solid line, Width `0.5`, Opacity `0.5`.
    * **Direction:** Horizontal only (for categorical X) or Both (for scatter/continuous).
3.  **Legend:**
    * **Frame:** No border (Frameon=`False`).
    * **Position:** Top-right (internal) or Top-left (external), depending on whitespace.
4.  **Geometry:**
    * **Line Width (Plots):** `2.5pt` (standardizes thickness across libraries).
    * **Marker Size:** `8pt`.

---

## 5. Theme Specifications

### ☀️ Light Theme (Professional & Clean)
Best for reports, PDFs, and white-paper presentations.

| Component | Hex Code / Value | Notes |
| :--- | :--- | :--- |
| **Figure Background** | `#FFFFFF` | Pure White |
| **Plot Area Background**| `#FFFFFF` | Pure White |
| **Text Color (Primary)**| `#1F2937` | Dark Grey (Not Black) |
| **Text Color (Secondary)**| `#6B7280` | Medium Grey (Ticks) |
| **Axis Spines** | `#374151` | Dark Slate |
| **Gridlines** | `#E5E7EB` | Very Light Grey |

> **Implementation Note:** Ensure text is never `#000000`. The harsh contrast causes eye strain. Use `#1F2937`.

### 🌙 Dark Theme (Sleek & Cyber)
Best for dashboards, screen-based presentations, and dark-mode UIs.

| Component | Hex Code / Value | Notes |
| :--- | :--- | :--- |
| **Figure Background** | `#111827` | Deep Blue-Black |
| **Plot Area Background**| `#111827` | Matches Figure |
| **Text Color (Primary)**| `#F9FAFB` | Off-White |
| **Text Color (Secondary)**| `#9CA3AF` | Light Grey |
| **Axis Spines** | `#D1D5DB` | Light Grey |
| **Gridlines** | `#374151` | Dark Grey (Low Contrast) |

> **Implementation Note:** In dark mode, reduce the opacity of filled areas (e.g., area charts) by 10% compared to light mode to prevent "glowing" effects that reduce readability.

---

## 6. Library Implementation Nuances

To achieve the "Universal" look defined above, specific settings must be toggled in each library:

### **Matplotlib / Seaborn**
Seaborn is built on Matplotlib. To ensure they match, modify the `plt.rcParams` dictionary directly rather than using `sns.set_style`.
* **Grid:** `axes.grid: True`, `grid.linestyle: -`
* **Spines:** `axes.spines.top: False`, `axes.spines.right: False`
* **Font:** `font.family: sans-serif`
* **Colors:** Pass the Hex List to `axes.prop_cycle`.

### **Plotly**
Plotly defaults to an interactive web look. You must override the template.
* **Layout:** Use `layout.template = "plotly_white"` (for light) or `"plotly_dark"` (for dark) as a base, then override.
* **Backgrounds:** Explicitly set `paper_bgcolor` and `plot_bgcolor` to the Hex codes defined above.
* **Margins:** Plotly adds excessive padding. Set `margin=dict(t=50, l=50, r=20, b=50)` to match the tighter Matplotlib look.

---

### Summary Checklist
Before finalizing a plot, ask:
* [ ] Is the background strictly `#FFFFFF` or `#111827`?
* [ ] Are the Top and Right borders removed?
* [ ] Is the font Sans-Serif?
* [ ] Are the colors drawn strictly from the Hex palette?
* [ ] Is the title left-aligned?