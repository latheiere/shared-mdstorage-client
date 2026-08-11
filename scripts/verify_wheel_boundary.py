from __future__ import annotations

import pathlib
import tomllib
import zipfile


def main() -> None:
    project = tomllib.loads(pathlib.Path("pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]
    distribution = str(project["name"]).replace("-", "_")
    version = str(project["version"])
    wheels = sorted(pathlib.Path("dist").glob(f"{distribution}-{version}-*.whl"))
    if len(wheels) != 1:
        raise SystemExit("build must produce exactly one wheel for the project version")

    expected_roots = {distribution, f"{distribution}-{version}.dist-info"}
    with zipfile.ZipFile(wheels[0]) as archive:
        roots = {
            name.split("/", 1)[0]
            for name in archive.namelist()
            if name and not name.startswith("/")
        }
    if roots != expected_roots:
        raise SystemExit(
            "wheel content crosses the package boundary: "
            f"expected {sorted(expected_roots)!r}, found {sorted(roots)!r}"
        )


if __name__ == "__main__":
    main()
