from __future__ import annotations

import argparse
import pathlib
import sys
import yaml

ROOT = pathlib.Path(__file__).resolve().parent

def load_yaml(path: pathlib.Path) -> dict:
    """Load a YAML file and return its contents as a dictionary."""
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def dump_yaml(obj: dict, path: pathlib.Path) -> None:
    """Dump a dictionary to a YAML file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(obj, f, sort_keys=False, width=120)

def merge_repos(configs: list[str]) -> list[dict]:
    """Merge the 'repos' lists from multiple config files."""
    repos: list[dict] = []
    for name in configs:
        p = ROOT / "configs" / f"{name}.yaml"
        if not p.exists():
            raise FileNotFoundError(f"Config not found: {p}")
        data = load_yaml(p)
        r = data.get("repos", [])
        if not isinstance(r, list):
            raise ValueError(f"{p}: 'repos' must be a list")
        repos.extend(r)
    return repos

def main() -> int:
    """Main entry point."""
    ap = argparse.ArgumentParser(description="Generate pre-commit bundle configs.")
    ap.add_argument("--bundles", default=str(ROOT / "bundles.yaml"))
    ap.add_argument("--out", default=str(ROOT / "bundles"))
    args = ap.parse_args()

    bundles_path = pathlib.Path(args.bundles)
    out_dir = pathlib.Path(args.out)

    spec = load_yaml(bundles_path)
    bundles = spec.get("bundles", {})
    if not bundles:
        raise ValueError("bundles.yaml: missing 'bundles'")

    header = {
        "minimum_pre_commit_version": "3.0.0",
        "default_stages": ["pre-commit"],
    }

    for bundle_name, bundle_spec in bundles.items():
        configs = bundle_spec.get("configs", [])
        if not configs:
            raise ValueError(f"bundle '{bundle_name}': missing configs")

        config = dict(header)
        config["repos"] = merge_repos(configs)

        out_path = out_dir / f"{bundle_name}.yaml"
        dump_yaml(config, out_path)
        print(f"Wrote {out_path} (configs={configs})")

    return 0

if __name__ == "__main__":
    main()
    sys.exit(0)