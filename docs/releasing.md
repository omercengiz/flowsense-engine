# Releasing FlowSense

Release preparation and publication are intentionally separate. Merge the release
preparation pull request before creating a tag.

## Prepare

1. Update `version` in `pyproject.toml` according to Semantic Versioning.
2. Move notable changes into a dated section in `CHANGELOG.md`.
3. Run the same checks as CI:

   ```bash
   ruff check .
   ruff format --check .
   pyright
   python -m pytest -m "not integration"
   python -m build
   ```

4. Install the wheel in a clean environment and verify its commands:

   ```bash
   uv venv /tmp/flowsense-release-test
   uv pip install --python /tmp/flowsense-release-test/bin/python dist/*.whl
   /tmp/flowsense-release-test/bin/flowsense --version
   /tmp/flowsense-release-test/bin/flowsense --help
   ```

## Tag

After the release pull request is merged into `master` and CI passes:

```bash
git switch master
git pull --ff-only origin master
git tag -a v0.5.1 -m "FlowSense 0.5.1"
git push origin v0.5.1
```

Create and publish the corresponding GitHub Release after pushing the tag. The
release workflow builds that exact tag and publishes it through the protected
`pypi` environment. Package publication should run only after the built artifacts
have been verified.

Use `FlowSense 0.5.1` as the release title, select `Latest`, and do not mark a
stable release as a pre-release. Use [release-0.5.1.md](release-0.5.1.md) as the
GitHub release notes; the matching `CHANGELOG.md` section remains the canonical
change inventory.
