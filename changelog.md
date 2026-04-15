# Changelog

All notable changes to this project are documented in this file.

## [2.0.0] - 2026-04-15

### Rename and Product Identity

- Project name changed from **Bedrux** to **Minara**.
- This repository now tracks the Minara line as the primary product direction.
- Versioning reset for the new generation as **v2.0.0**.

### Architectural Upgrade

- Rebuilt from a Python/Shell TUI-oriented stack into a Rust-based web architecture.
- Introduced an async backend with **Axum + Tokio** and server-side rendering via **Askama**.
- Added API-driven UI interactions and real-time log streaming with SSE.

### Operations and Runtime Model

- Added first-class multi-instance Bedrock lifecycle management through a browser panel.
- Refined mode-aware runtime behavior:
  - Production mode focuses on stable launch policy.
  - Development mode (`-d`) enables architecture-aware launch flexibility and device-source editing.
- Added graceful process shutdown signaling for operator ergonomics.

### Integration Layer

- Standardized **Asmo** readiness and monitoring integration in startup and Device Info flows.
- Integrated **bionilux** launch pathways for Bedrock execution compatibility.
- Clarified ARM64 execution strategy around Box64/bionilux workflows.

### Storage and Data Model

- Migrated persistent storage from legacy Bedrux-style home paths to:
  - `~/.local/share/minara/instances`
  - `~/.local/share/minara/backups`
- Added startup bootstrap to ensure required data directories exist automatically.
- Strengthened backup lineage handling using instance identity metadata.

### UX and Documentation

- Expanded panel experience across dashboard, console, settings, files, device info, and backups.
- Introduced new polished README and visual media placeholders for Minara branding.

---

## Legacy Reference

Bedrux releases (`v1.x`) remain the historical foundation of this project line.
Minara `v2.0.0` is the modernization and upgrade path replacing Bedrux in this repository.
