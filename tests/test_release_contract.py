from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_release_workflows_preserve_tag_artifact_and_publisher_provenance() -> None:
    release = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    publisher = (ROOT / ".github/workflows/publish-pypi.yml").read_text(
        encoding="utf-8"
    )

    assert "expected = f\"v{project['version']}\"" in release
    assert "python -m build --no-isolation" in release
    assert "python scripts/verify_wheel_boundary.py" in release
    assert "__version__ == version(\"shared-mdstorage-client\")" in release
    assert "gh release create" in release
    assert "--draft" in release
    assert "gh release edit" in release
    assert "--clobber" not in release

    assert "ref: ${{ inputs.tag }}" in publisher
    assert "expected = f\"v{project['version']}\"" in publisher
    assert 'all_wheels = sorted(dist.glob("*.whl"))' in publisher
    assert 'all_sdists = sorted(dist.glob("*.tar.gz"))' in publisher
    assert "len(manifest_entries) != 2" in publisher
    assert "checksum manifest paths must be plain filenames" in publisher
    assert "sha256sum --check SHA256SUMS" in publisher
    assert "python scripts/verify_wheel_boundary.py" in publisher
    assert "name: pypi" in publisher
    assert "id-token: write" in publisher
    assert "pypa/gh-action-pypi-publish@release/v1" in publisher
