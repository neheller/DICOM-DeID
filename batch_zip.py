#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import zipfile
import time
from typing import List, Tuple

GB = 1024 ** 3

def human_bytes(n: int) -> str:
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024 or unit == "TB":
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"

def walk_size(p: Path) -> int:
    """Compute total size of a file or directory path. Skips broken symlinks."""
    if p.is_symlink():
        try:
            target = p.resolve(strict=True)
        except Exception:
            return 0
        p = target

    if p.is_file():
        try:
            return p.stat().st_size
        except Exception:
            return 0

    total = 0
    for root, dirs, files in os.walk(p, followlinks=False):
        # Optionally skip symlinked dirs
        dirs[:] = [d for d in dirs if not Path(root, d).is_symlink()]
        for name in files:
            fp = Path(root, name)
            if fp.is_symlink():
                # skip symlinked files to avoid surprises
                continue
            try:
                total += fp.stat().st_size
            except FileNotFoundError:
                # File may have disappeared
                continue
    return total

def list_children(source: Path) -> List[Path]:
    """List immediate children of source. Includes files and directories. Skips hidden only if user asked, but here we include all."""
    return sorted([p for p in source.iterdir() if p.exists()], key=lambda p: p.name.lower())

def best_fit_decreasing(items: List[Tuple[Path, int]], capacity: int):
    """
    Bin pack using Best Fit Decreasing.
    items is a list of (path, size).
    Returns a list of bins, each bin is a list of (path, size).
    Items larger than capacity go into their own bin and will be marked oversize.
    """
    # sort by size desc
    items_sorted = sorted(items, key=lambda t: t[1], reverse=True)
    bins: List[List[Tuple[Path,int]]] = []
    free_space: List[int] = []

    for path, size in items_sorted:
        if size > capacity:
            # oversize item gets its own bin
            bins.append([(path, size)])
            free_space.append(max(0, capacity - size))
            continue

        # find best fit bin
        best_idx = -1
        best_space_after = None
        for i, space in enumerate(free_space):
            if size <= space:
                after = space - size
                if best_space_after is None or after < best_space_after:
                    best_space_after = after
                    best_idx = i
        if best_idx == -1:
            # create new bin
            bins.append([(path, size)])
            free_space.append(capacity - size)
        else:
            bins[best_idx].append((path, size))
            free_space[best_idx] -= size

    return bins

def zip_batch(batch_index: int, items: List[Tuple[Path,int]], source_root: Path, dest: Path, compression_level: int = 6) -> Path:
    """
    Create a zip file for a batch.
    Items are top level children under source_root. We add them preserving relative paths under their names.
    """
    dest.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    name = f"batch_{batch_index:03d}_{ts}.zip"
    zpath = dest / name

    # Use deflated compression with ZIP64 for large archives
    comp = zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(zpath, mode="w", compression=comp, compresslevel=compression_level, allowZip64=True) as zf:
        for child, _ in items:
            if child.is_file():
                arcname = child.name
                zf.write(child, arcname=arcname)
            else:
                # add directory contents
                for root, dirs, files in os.walk(child, followlinks=False):
                    # write directories so empty dirs are preserved
                    rel = Path(root).relative_to(source_root)
                    if not files and not dirs:
                        zinfo = zipfile.ZipInfo(str(rel) + "/")
                        zf.writestr(zinfo, "")
                    for fname in files:
                        fp = Path(root) / fname
                        if fp.is_symlink():
                            # skip symlinks
                            continue
                        arcname = str(Path(root).relative_to(source_root) / fname)
                        zf.write(fp, arcname=arcname)
    return zpath

def plan_and_zip(source: Path, dest: Path, batch_size_gb: float, dry_run: bool, compression_level: int):
    capacity = int(batch_size_gb * GB)
    children = list_children(source)
    if not children:
        print("No children found in source. Nothing to do.")
        return

    print(f"Scanning sizes under: {source}")
    items: List[Tuple[Path,int]] = []
    for child in children:
        size = walk_size(child)
        items.append((child, size))
        print(f" - {child.name}: {human_bytes(size)}")

    total_size = sum(s for _, s in items)
    print(f"Total size of all children: {human_bytes(total_size)}")
    print(f"Batch capacity: {batch_size_gb:.2f} GB")

    bins = best_fit_decreasing(items, capacity)

    print("")
    print("Planned batches:")
    for i, b in enumerate(bins, start=1):
        bsize = sum(s for _, s in b)
        oversize = any(s > capacity for _, s in b)
        print(f" Batch {i:03d} - {human_bytes(bsize)}" + (" - contains oversize item" if oversize else ""))
        for p, s in b:
            print(f"    {p.name} - {human_bytes(s)}")
    print(f"Estimated number of zips: {len(bins)}")

    if dry_run:
        print("")
        print("Dry run was requested. No archives were created.")
        return

    print("")
    print(f"Writing zips to: {dest}")
    created = []
    for i, b in enumerate(bins, start=1):
        zpath = zip_batch(i, b, source_root=source, dest=dest, compression_level=compression_level)
        bsize = sum(s for _, s in b)
        created.append((zpath, bsize))
        print(f" Created {zpath.name} - includes {len(b)} item(s) - approx payload size {human_bytes(bsize)}")

    print("")
    print("Done.")
    print("Created archives:")
    for zpath, bsize in created:
        print(f" - {zpath} - approx payload size {human_bytes(bsize)}")

def main():
    parser = argparse.ArgumentParser(
        description="Group immediate children of a folder into zip batches no larger than a target size."
    )
    parser.add_argument("source", type=Path, help="Path to the source folder that contains subfolders or files")
    parser.add_argument("destination", type=Path, help="Path to the folder where zips will be written")
    parser.add_argument("--batch-size-gb", type=float, default=10.0, help="Maximum size per batch in GB")
    parser.add_argument("--dry-run", action="store_true", help="Plan and print batches without creating zip files")
    parser.add_argument("--compression-level", type=int, default=6, choices=range(0,10),
                        help="Deflate compression level 0 to 9")
    args = parser.parse_args()

    source = args.source.expanduser().resolve()
    dest = args.destination.expanduser().resolve()

    if not source.exists() or not source.is_dir():
        print(f"Source does not exist or is not a directory: {source}")
        return

    plan_and_zip(source, dest, args.batch_size_gb, args.dry_run, args.compression_level)


if __name__ == "__main__":
    main()
