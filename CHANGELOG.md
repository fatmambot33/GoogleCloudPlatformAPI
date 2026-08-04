# Changelog

All notable changes to this project are documented here.

The format follows Keep a Changelog principles and the project uses semantic versioning.

## Unreleased

## 2.4.0 - 2026-08-04

### Added

- Standard product, roadmap, contribution, security, support, and release policies.
- Public API consistency, authentication, and error-handling policies.
- PEP 561 `py.typed` marker for downstream type checkers.
- Distribution build and validation in CI.
- Python 3.12 CI coverage.

### Changed

- Expanded the product vision and decision framework.
- Standardized roadmap and contribution processes.
- Modernized GitHub Actions and enabled pip caching.
- Stabilized development tooling by pinning the formatter.

### Fixed

- Removed the obsolete pytest-pydocstyle plugin that was incompatible with current pytest releases.

### Deferred

- Raising coverage from 70.84% to 90% remains tracked in issue #29 and is not represented as complete in this release.
