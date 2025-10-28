# petl/transform/standardize.py
from __future__ import annotations
from typing import Iterable, Iterator, List, Sequence, Tuple, Union, Optional, Any
import math


Row = Sequence[Any]
Table = Iterable[Row]
Field = Union[str, int]
Fields = Union[Field, Sequence[Field]]


def _as_list(x: Fields) -> List[Field]:
    if isinstance(x, (list, tuple)):
        return list(x)
    return [x]


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _mean_std(values: List[float], ddof: int = 0) -> Tuple[float, float]:
    if not values:
        return 0.0, 0.0
    m = sum(values) / len(values)
    var = sum((v - m) ** 2 for v in values) / max(1, len(values) - ddof)
    return m, math.sqrt(var)


def standardize(
    table: Table,
    fields: Fields,
    newfields: Optional[Fields] = None,
    ddof: int = 0,
) -> Table:

    """
    Return a new table with the given numeric field(s) standardized to mean 0 and std 1.

    Parameters
    ----------
    table : PETL-style table (first row is header)
    fields : str | int | sequence
        Column name(s) to standardize.
    newfields : Optional[str | sequence]
        If provided, write standardized values into these new columns
        (same length as `fields`). Otherwise, overwrite the original columns.
    ddof : int
        Delta degrees of freedom for std. Use 0 for population (default), 1 for sample.

    Notes
    -----
    - Non-numeric or None values are passed through unchanged.
    - If std is 0 (constant column), standardized values are 0.0.
    """
    fields_list = _as_list(fields)
    if newfields is not None:
        newfields_list = _as_list(newfields)
        if len(newfields_list) != len(fields_list):
            raise ValueError("newfields must be the same length as fields")
    else:
        newfields_list = None

    # Materialize the table so we can compute stats and then yield again.
    rows = list(table)
    if not rows:
        return rows  # empty table

    header = list(rows[0])
    # Map field names to indices
    name_to_idx = {name: i for i, name in enumerate(header)}

    idxs: List[int] = []
    for f in fields_list:
        if isinstance(f, int):
            idxs.append(f)
        else:
            if f not in name_to_idx:
                raise KeyError(f"field {f!r} not found in header")
            idxs.append(name_to_idx[f])

    # Compute mean/std for each selected field
    col_stats: List[Tuple[float, float]] = []
    data_rows = rows[1:]
    for ci in idxs:
        nums = [float(r[ci]) for r in data_rows if _is_number(r[ci])]
        m, s = _mean_std(nums, ddof=ddof)
        col_stats.append((m, s))

    # If adding new fields, extend header
    if newfields_list:
        header_out = header + list(newfields_list)
    else:
        header_out = header

    def _iter() -> Iterator[Row]:
        # yield header
        yield tuple(header_out)

        for r in data_rows:
            r_list = list(r)

            # compute standardized values for each selected column
            zvals: List[Optional[float]] = []
            for (ci, (m, s)) in zip(idxs, col_stats):
                val = r_list[ci]
                if _is_number(val):
                    if s == 0 or math.isnan(s):
                        z = 0.0
                    else:
                        z = (float(val) - m) / s
                    zvals.append(z)
                else:
                    zvals.append(val)  # keep as-is (e.g., None or text)

            if newfields_list:
                # append new z-values at the end
                yield tuple(r_list + zvals)
            else:
                # overwrite original fields in-place
                for offset, ci in enumerate(idxs):
                    r_list[ci] = zvals[offset]
                yield tuple(r_list)

    return _iter()


if __name__ == "__main__":
    # Example usage
    import petl as etl

    table = [
        ('id', 'score'),
        (1, 50),
        (2, 60),
        (3, 70),
    ]

    result = etl.standardize(table, 'score')
    for row in result:
        print(row)
