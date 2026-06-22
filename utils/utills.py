"""
utils/file_utils.py
====================
Reusable helpers for locating the most recent source-data export(s) in a
drop folder. The original notebooks repeated this glob + getctime pattern
4 separate times with copy-pasted logic and silent "No matching file found"
prints; consolidated here as two well-defined, testable functions that raise
a clear exception instead of letting downstream code fail on an undefined
variable.
"""

import glob
import os
from pathlib import Path
from typing import List


class SourceFileNotFoundError(FileNotFoundError):
    """Raised when no file matches an expected source-data pattern."""


def get_latest_file(folder: Path, pattern: str) -> Path:
    """
    Return the most recently created file in `folder` matching `pattern`.

    Parameters
    ----------
    folder : Path
        Directory to search in (e.g. config.SOURCE_DIR).
    pattern : str
        Glob pattern, e.g. "CreditBookLoanData*.xlsx".

    Returns
    -------
    Path
        Path to the newest matching file.

    Raises
    ------
    SourceFileNotFoundError
        If no file matches the pattern. This is intentionally a hard
        failure -- the original notebooks would silently leave the
        downstream DataFrame undefined and crash with an opaque
        NameError several cells later.
    """
    matches = glob.glob(os.path.join(folder, pattern))
    if not matches:
        raise SourceFileNotFoundError(
            f"No file matching pattern '{pattern}' found in '{folder}'."
        )
    return Path(max(matches, key=os.path.getctime))


def get_latest_two_files(folder: Path, pattern: str) -> List[Path]:
    """
    Return the two most recently created files matching `pattern`, newest
    first. Used by the KYB ingestion step, which needs to diff "today's"
    file against "yesterday's" file to backfill any compliance fields that
    regressed to a non-final status between runs.

    Raises
    ------
    SourceFileNotFoundError
        If fewer than two matching files exist.
    """
    matches = glob.glob(os.path.join(folder, pattern))
    if len(matches) < 2:
        raise SourceFileNotFoundError(
            f"Expected at least 2 files matching '{pattern}' in '{folder}' "
            f"(need latest + previous), found {len(matches)}."
        )
    matches.sort(key=os.path.getctime, reverse=True)
    return [Path(matches[0]), Path(matches[1])]
