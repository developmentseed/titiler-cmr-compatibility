import earthaccess
import pandas as pd
import numpy as np

from typing import Dict, Optional, Tuple, Any

# ----------------------------------------
# Helpers to parse metadata from earthaccess
# ----------------------------------------


def parse_temporal(umm: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    temporal = umm.get("TemporalExtent", {})
    range_date_time = temporal.get("RangeDateTime", {})
    begin = range_date_time.get("BeginningDateTime", None)
    end = range_date_time.get("EndingDateTime", None)
    return begin, end


def parse_bounds_from_spatial(
    umm: Dict[str, Any],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    spatial = umm.get("SpatialExtent", {}) or {}
    horiz = spatial.get("HorizontalSpatialDomain", {}) or {}
    geom = horiz.get("Geometry", {}) or {}

    # 1) Bounding rectangles
    rects = geom.get("BoundingRectangles") or []
    if rects:
        wests = [r.get("WestBoundingCoordinate") for r in rects if r]
        easts = [r.get("EastBoundingCoordinate") for r in rects if r]
        souths = [r.get("SouthBoundingCoordinate") for r in rects if r]
        norths = [r.get("NorthBoundingCoordinate") for r in rects if r]
        if all(len(lst) > 0 for lst in (wests, easts, souths, norths)):
            return (
                float(np.min(wests)),
                float(np.min(souths)),
                float(np.max(easts)),
                float(np.max(norths)),
            )

    # 2) GPolygons
    gpolys = geom.get("GPolygons") or []
    coords_w, coords_e, coords_s, coords_n = [], [], [], []
    for gp in gpolys:
        b = gp.get("Boundary", {})
        pts = b.get("Points", [])
        lons = [p.get("Longitude") for p in pts if p and p.get("Longitude") is not None]
        lats = [p.get("Latitude") for p in pts if p and p.get("Latitude") is not None]
        if lons and lats:
            coords_w.append(np.min(lons))
            coords_e.append(np.max(lons))
            coords_s.append(np.min(lats))
            coords_n.append(np.max(lats))
    if coords_w and coords_e and coords_s and coords_n:
        return (
            float(np.min(coords_w)),
            float(np.min(coords_s)),
            float(np.max(coords_e)),
            float(np.max(coords_n)),
        )

    return None, None, None, None
    