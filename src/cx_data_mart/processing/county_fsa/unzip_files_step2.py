"""Unzip files."""

import zipfile
from pathlib import Path

DOWNLOAD_DIR = Path(__file__).parent / "county_fsa_downloads"


def safe_extract(zf: zipfile.ZipFile, target_dir: Path) -> None:
    for member in zf.infolist():
        member_path = target_dir / member.filename
        if not str(member_path.resolve()).startswith(str(target_dir.resolve())):
            raise RuntimeError(f"Blocked path traversal attempt in `{member.filename}`")
        if member.is_dir():
            member_path.mkdir(parents=True, exist_ok=True)
        else:
            member_path.parent.mkdir(parents=True, exist_ok=True)
            with (
                zf.open(member) as src,
                open(  # noqa: FURB103
                    member_path, "wb"
                ) as dst,
            ):
                dst.write(src.read())


def unzip_all(source_dir: Path = DOWNLOAD_DIR, *, overwrite: bool = False) -> None:
    source_dir.mkdir(parents=True, exist_ok=True)
    for zip_path in source_dir.glob("*.zip"):
        extract_dir = source_dir / zip_path.stem
        if extract_dir.exists() and not overwrite:
            continue
        if extract_dir.exists() and overwrite:
            for p in sorted(extract_dir.rglob("*"), reverse=True):
                if p.is_file():
                    p.unlink()
                else:
                    p.rmdir()
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path) as zf:
            safe_extract(zf, extract_dir)


if __name__ == "__main__":
    unzip_all()
