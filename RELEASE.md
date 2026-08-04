# Release Process

GoogleCloudPlatformAPI follows semantic versioning.

## Release Types

- Patch: backward-compatible fixes and documentation corrections.
- Minor: backward-compatible features and meaningful improvements.
- Major: intentional breaking changes with migration guidance.

## Checklist

1. Confirm the milestone is complete and all included issues are closed.
2. Review changes against `PRODUCT.md`.
3. Confirm public APIs are documented, typed, and tested.
4. Run all required checks from `CONTRIBUTING.md`.
5. Update the version in `pyproject.toml`.
6. Move relevant entries from `Unreleased` into a dated version section in `CHANGELOG.md`.
7. Build and inspect the source and wheel distributions.
8. Publish the release through the approved CI workflow.
9. Create the GitHub release and verify installation from PyPI.

## Release Rules

- Do not release with failing required checks.
- Do not include unrelated changes in a release preparation pull request.
- Breaking changes require a major version and clear migration notes.
- Prefer small, predictable releases over large batches.
