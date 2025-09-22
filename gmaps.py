import asyncio
import re
import math
import logging
import random
import base64
import json
import duckdb
from urllib.parse import unquote_plus
from datetime import datetime, UTC, timedelta
import nodriverplus
from nodriverplus import cdp

logging.basicConfig(level=logging.INFO)

# experimental request interception wiring
from nodriverplus import RequestPausedHandler

conn = duckdb.connect("places.duckdb")
conn.execute(
    "CREATE TABLE IF NOT EXISTS places (search_term TEXT, name TEXT, address TEXT, website TEXT, place_id TEXT PRIMARY KEY, cid TEXT, latitude REAL, longitude REAL, phone TEXT, timezone TEXT, language TEXT, country TEXT, review_count INTEGER, average_rating REAL)"
)

# tiles table persists hierarchical tile state
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS tiles (
        search_term TEXT,
        z INTEGER,
        x INTEGER,
        y INTEGER,
        status TEXT,
        passes INTEGER DEFAULT 0,
        new_last_pass INTEGER DEFAULT 0,
        total INTEGER DEFAULT 0,
        consecutive_zero INTEGER DEFAULT 0,
        expanded BOOLEAN DEFAULT FALSE,
        feed_complete BOOLEAN DEFAULT FALSE,
        last_count INTEGER DEFAULT 0,
        last_saturated BOOLEAN DEFAULT FALSE,
        last_visit_ts TIMESTAMP,
        PRIMARY KEY(search_term, z, x, y)
    )
    """
)

# single-row persistent state for resuming scraper position (includes search term)
TILE_SIZE = 256
MAX_LAT = 85.05112878
LAT_EDGE_PADDING = 0.01
_VIEWPORT_CACHE = {"ts": 0.0, "data": None}
CLAMP_LAT_DELTA = 0.00008
# pan tolerance tuning constants (zoom 1 = most zoomed out, 21 = most zoomed in)
PAN_BASE_DEG = 3  # baseline degrees tolerance at zoom 1
PAN_EXP_DECAY = 0.2  # exponential decay rate per zoom level
PAN_MIN_DEG = 0.00035  # hard floor to avoid overly tiny tolerance
PAN_STALL_GROWTH = 2.0  # multiplier applied when repeated stalls
PAN_STALL_THRESHOLD = 2  # number of non-improving iterations before growth
# removed exploration zoom constant; zoom now stays where gestures leave it

conn.execute(
    "CREATE TABLE IF NOT EXISTS map_state (lat REAL, lon REAL, zoom REAL, pan_direction TEXT, search_term TEXT, last_lat_direction TEXT, current_tile_z INTEGER, current_tile_x INTEGER, current_tile_y INTEGER, updated_at TIMESTAMP)"
)


class MapScraper(RequestPausedHandler):
    async def should_intercept_response(self, ev):
        return ev.request.url.startswith("https://www.google.com/search?tbm=map&")

    async def should_take_response_body_as_stream(self, ev):
        return True

    async def on_stream_finished(self, ev):
        search_term = re.search(r"[?&]q=([^&]+)", ev.request.url)
        search_term = search_term.group(1) if search_term else ""
        search_term = unquote_plus(search_term)
        body_bytes = base64.b64decode(ev.body)
        body_str = body_bytes.decode("utf-8").removesuffix('/*""*/')
        body = json.loads(body_str)
        data = body["d"].removeprefix(")]}'\n")
        data = json.loads(data)
        if data[64] is None:
            return

        new_places = 0
        # collect all place_ids to check existence in batch
        place_ids = []
        for p in data[64]:
            try:
                p = p[1]
                place_id = p[10]
                place_ids.append(place_id)
            except Exception as e:
                with open("data_debug.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logging.error(
                    "error processing place data: %s\n  p = %s\ndata saved to data_debug.json", e, p
                )

        # query existing place_ids in batch
        if place_ids:
            existing_place_ids = set(
                row[0]
                for row in conn.execute(
                    "SELECT place_id FROM places WHERE place_id IN ?",
                    (tuple(place_ids),),
                ).fetchall()
            )
        else:
            existing_place_ids = set()

        # collect new places for batch insert
        new_places_data = []
        for p in data[64]:
            p = p[1]
            place_id = p[10]
            cid_str = place_id.split(":")[1]
            cid = str(int(cid_str, 16))
            if place_id in existing_place_ids:
                continue
            latitude = p[9][2] if p[9] and len(p[9]) > 2 else None
            longitude = p[9][3] if p[9] and len(p[9]) > 3 else None
            phone = (
                p[178][0][3]
                if p[178] and len(p[178]) > 0 and p[178][0] and len(p[178][0]) > 3
                else None
            )
            timezone = p[30] if len(p) > 30 else None
            language = p[110] if len(p) > 110 else None
            country = p[243] if len(p) > 243 else None
            review_count = p[4][8] if p[4] and len(p[4]) > 8 else None
            average_rating = p[4][7] if p[4] and len(p[4]) > 7 else None
            new_places_data.append(
                (
                    search_term,
                    p[11],
                    p[39],
                    p[7][0] if p[7] and len(p[7]) > 0 else None,
                    place_id,
                    cid,
                    latitude,
                    longitude,
                    phone,
                    timezone,
                    language,
                    country,
                    review_count,
                    average_rating,
                )
            )

        # batch insert new places
        if new_places_data:
            conn.executemany(
                "INSERT INTO places VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                new_places_data,
            )
            new_places = len(new_places_data)
        else:
            new_places = 0

        count = conn.execute("SELECT COUNT(*) FROM places").fetchone()[0]
        if new_places > 0:
            logging.debug(
                "found %d new places (%d total places).",
                new_places,
                count,
            )


def save_map_state(
    lat: float | None,
    lon: float | None,
    zoom: float | None,
    pan_direction: str,
    search_term: str | None,
    last_lat_direction: str | None,
    current_tile_z: int | None = None,
    current_tile_x: int | None = None,
    current_tile_y: int | None = None,
):
    # overwrite single row
    conn.execute("DELETE FROM map_state")
    conn.execute(
        "INSERT INTO map_state VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
        (
            lat,
            lon,
            zoom,
            pan_direction,
            search_term,
            last_lat_direction,
            current_tile_z,
            current_tile_x,
            current_tile_y,
        ),
    )


def _ease_in_out(t: float) -> float:
    return 2 * t * t if t < 0.5 else -1 + (4 - 2 * t) * t


async def scroll(
    tab,
    x: float | int,
    y: float | int,
    scroll_down=True,
    steps=7,
    step_px=100,
    step_duration=0.1,
):
    total_time = step_duration * steps
    # reuse shared easing
    bezier_times = (
        [0] if steps == 1 else [_ease_in_out(i / (steps - 1)) for i in range(steps)]
    )

    for i in range(steps):
        await tab.send(
            cdp.input_.dispatch_mouse_event(
                type_="mouseWheel",
                x=x,
                y=y,
                delta_x=0,
                delta_y=step_px if scroll_down else -step_px,
            )
        )
        if i < steps - 1:
            # wait a non-linear interval
            dt = (bezier_times[i + 1] - bezier_times[i]) * total_time
            jitter = 0.1 * random.random()
            await asyncio.sleep(dt * jitter)


def _cubic_bezier(p0, p1, p2, p3, t):
    mt = 1.0 - t
    x = (
        (mt * mt * mt) * p0[0]
        + 3 * (mt * mt) * t * p1[0]
        + 3 * mt * (t * t) * p2[0]
        + (t * t * t) * p3[0]
    )
    y = (
        (mt * mt * mt) * p0[1]
        + 3 * (mt * mt) * t * p1[1]
        + 3 * mt * (t * t) * p2[1]
        + (t * t * t) * p3[1]
    )
    return x, y


async def drag_bezier(
    tab,
    x0: float | int,
    y0: float | int,
    x1: float | int,
    y1: float | int,
    *,
    steps: int = 20,
    duration: float = 0.6,
    jitter: float = 1.5,
    handle: float = 0.1,  # handle length as a fraction of distance
    bulge: float | None = 0.1,  # perpendicular offset in px; default = 0.1 * distance
    bulge_dir: int | None = None,  # 1 for one side, -1 for the other; None = random
    cp1: (
        tuple[float, float] | None
    ) = None,  # explicit control pt 1 (overrides handle/bulge)
    cp2: (
        tuple[float, float] | None
    ) = None,  # explicit control pt 2 (overrides handle/bulge)
    button=cdp.input_.MouseButton.LEFT,
):
    await tab.send(
        cdp.input_.dispatch_mouse_event(
            type_="mousePressed", x=x0, y=y0, button=button, click_count=1
        )
    )

    # compute control points if not provided
    p0, p3 = (x0, y0), (x1, y1)
    dx, dy = (x1 - x0), (y1 - y0)
    dist = math.hypot(dx, dy)
    if dist == 0:
        # trivial click
        await tab.send(
            cdp.input_.dispatch_mouse_event(
                type_="mouseReleased", x=x1, y=y1, button=button, click_count=1
            )
        )
        return

    steps = max(2, int(steps))
    times = [_ease_in_out(i / (steps - 1)) for i in range(steps)]

    # choose drag path: straight or bezier
    if bulge is None:

        def drag_point(t):
            x = x0 + (x1 - x0) * t
            y = y0 + (y1 - y0) * t
            return x, y

    else:
        vx, vy = dx / dist, dy / dist
        nx, ny = -vy, vx
        h = dist * max(0.0, handle)
        sgn = (
            bulge_dir if bulge_dir in (-1, 1) else (1 if random.random() < 0.5 else -1)
        )
        bx, by = nx * bulge * sgn, ny * bulge * sgn
        cp1 = cp1 or (x0 + vx * h + bx, y0 + vy * h + by)
        cp2 = cp2 or (x1 - vx * h + bx, y1 - vy * h + by)

        def drag_point(t):
            return _cubic_bezier(p0, cp1, cp2, p3, t)

    for i, t in enumerate(times):
        x, y = drag_point(t)
        if i < steps - 1 and jitter > 0:
            x += random.uniform(-jitter, jitter)
            y += random.uniform(-jitter, jitter)
        await tab.send(
            cdp.input_.dispatch_mouse_event(type_="mouseMoved", x=x, y=y, button=button)
        )
        if i < steps - 1:
            dt = (times[i + 1] - times[i]) * duration
            dt = random.uniform(0.90, 1.10) * dt
            await asyncio.sleep(max(0.001, dt))

    await tab.send(
        cdp.input_.dispatch_mouse_event(
            type_="mouseReleased", x=x1, y=y1, button=button, click_count=1
        )
    )


async def get_dimensions(tab, selector, js_timeout_ms: int = None):
    js = _build_wait_for_selector_js(selector, center=False, timeout_ms=js_timeout_ms)
    rect_json = await tab.evaluate(js, await_promise=True)
    if not rect_json:
        raise asyncio.TimeoutError()
    if isinstance(rect_json, cdp.runtime.ExceptionDetails):
        logging.info(js)
        logging.info(rect_json)
    return json.loads(rect_json)


def _build_wait_for_selector_js(
    selector: str, center: bool = True, timeout_ms: int | None = None
) -> str:
    # promise resolving to either center string "x,y" or full rect json
    body = (
        "const b=x.getBoundingClientRect();res([(b.right+b.left)/2,(b.bottom+b.top)/2].join(','))"
        if center
        else "const b=x.getBoundingClientRect();res(JSON.stringify(b))"
    )
    to = 0 if timeout_ms is None else timeout_ms
    return (
        "new Promise((res)=>{const s='"
        + selector
        + "';let e=document.querySelector(s);const d=x=>setTimeout(()=>{"
        + body
        + "},1000);if(e) return d(e);const t=setTimeout(()=>res(''),"
        + str(to if to else 5000)
        + ");new MutationObserver((_,o)=>{e=document.querySelector(s);if(e){o.disconnect();clearTimeout(t);d(e)}}).observe(document.body,{childList:true,subtree:true})})"
    )


async def wait_for_selector_center(tab, selector: str, timeout_ms: int = 5000):
    js = _build_wait_for_selector_js(selector, center=True, timeout_ms=timeout_ms)
    coords = await tab.evaluate(js, await_promise=True)
    if not coords:
        raise asyncio.TimeoutError()
    if isinstance(coords, cdp.runtime.ExceptionDetails):
        logging.info(js)
        logging.info(coords)
    return [float(v) for v in coords.split(",")]


async def get_map_viewport_center(tab):
    js = "f=document.querySelector('[role=feed]');if(!f){[innerWidth/2, innerHeight/2].join(',')}else{f=f.getBoundingClientRect();[f.right + (innerWidth - f.right)/2, innerHeight/2].join(',')}"
    res = await tab.evaluate(js)
    if isinstance(res, cdp.runtime.ExceptionDetails):
        logging.info(js)
        logging.info(res)
        raise RuntimeError("failed to compute map viewport center")
    if not res:
        raise RuntimeError("map viewport center evaluation returned empty result")
    return [float(v) for v in res.split(",")]


async def zoom_out_to_limit(tab, center, max_attempts: int = 20):
    if center is None or any(c is None for c in center):
        raise ValueError("map center required for zoom out routine")
    last_zoom = lat_lon_zoom(tab.url)[2]
    if last_zoom is None:
        return None
    settled = 0
    best_zoom = last_zoom
    for _ in range(max_attempts):
        await settle_map_state(tab, ignore=["refresh", "scroll", "no-feed"])
        await scroll(
            tab,
            x=center[0] + random.uniform(-40, 40),
            y=center[1] + random.uniform(-40, 40),
            scroll_down=True,
            steps=7,
            step_duration=0.01,
        )
        await asyncio.sleep(random.uniform(0.5, 0.6))
        current_zoom = lat_lon_zoom(tab.url)[2]
        if current_zoom is None:
            break
        best_zoom = current_zoom
        if abs(current_zoom - last_zoom) < 0.01:
            settled += 1
        else:
            settled = 0
        if settled >= 2:
            break
        last_zoom = current_zoom
    return int(round(best_zoom)) if best_zoom is not None else None


def _latlon_to_tile(lat: float, lon: float, z: int) -> tuple[int, int]:
    # convert lat/lon to xyz tile indices for web mercator
    lat = _clamp(lat, -MAX_LAT, MAX_LAT)
    n = 2**z
    x = int((lon + 180.0) / 360.0 * n) % n
    lat_rad = math.radians(lat)
    y = int((1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2 * n)
    y = max(0, min(n - 1, y))
    return x, y


def ensure_initial_frontier(
    search_term: str, lat: float, lon: float, zoom: float
) -> tuple[int, int, int]:
    # insert only the single tile containing current lat/lon (lazy frontier start)
    if lat is None or lon is None or zoom is None:
        return 0, 0, 0
    z = int(max(0, math.floor(zoom)))
    x, y = _latlon_to_tile(lat, lon, z)
    exists = conn.execute(
        "SELECT 1 FROM tiles WHERE search_term=? AND z=? AND x=? AND y=? LIMIT 1",
        (search_term, z, x, y),
    ).fetchone()
    if exists:
        return 0, z, 1
    conn.execute(
        "INSERT INTO tiles (search_term, z, x, y, status) VALUES (?, ?, ?, ?, ?)",
        (search_term, z, x, y, "pending"),
    )
    return 1, z, 1


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _normalize_longitude(lon: float | None) -> float | None:
    if lon is None:
        return None
    return ((lon + 180.0) % 360.0) - 180.0


def mercator_tile_center(z: int, x: int, y: int) -> tuple[float, float]:
    denom = 1 << int(z)
    lon = (x + 0.5) / denom * 360.0 - 180.0
    n = math.pi * (1 - 2 * (y + 0.5) / denom)
    lat = math.degrees(math.atan(math.sinh(n)))
    lat = _clamp(lat, -MAX_LAT + LAT_EDGE_PADDING, MAX_LAT - LAT_EDGE_PADDING)
    return lat, lon


def mercator_tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    # returns (min_lat, max_lat, min_lon, max_lon) clipped to mercator safe range
    denom = 1 << int(z)
    lon_left = x / denom * 360.0 - 180.0
    lon_right = (x + 1) / denom * 360.0 - 180.0

    def y_to_lat(tile_y: float) -> float:
        n = math.pi * (1 - 2 * tile_y / denom)
        lat = math.degrees(math.atan(math.sinh(n)))
        return _clamp(lat, -MAX_LAT + LAT_EDGE_PADDING, MAX_LAT - LAT_EDGE_PADDING)

    lat_top = y_to_lat(y)
    lat_bottom = y_to_lat(y + 1)
    min_lat, max_lat = min(lat_bottom, lat_top), max(lat_bottom, lat_top)
    return (min_lat, max_lat, lon_left, lon_right)


def next_pending_tile(search_term: str):
    row = conn.execute(
        "SELECT z,x,y FROM tiles WHERE search_term=? AND (status IN ('pending', 'in_progress') OR (status='completed' AND (feed_complete = FALSE OR expanded = FALSE))) ORDER BY CASE WHEN status = 'in_progress' THEN 0 ELSE 1 END, passes, last_visit_ts NULLS FIRST LIMIT 1",
        (search_term,),
    ).fetchone()
    return row if row else None


def mark_tile_status(search_term: str, z: int, x: int, y: int, status: str):
    conn.execute(
        "UPDATE tiles SET status=?, last_visit_ts=CURRENT_TIMESTAMP WHERE search_term=? AND z=? AND x=? AND y=?",
        (status, search_term, z, x, y),
    )


def update_tile_pass_metrics(search_term: str, z: int, x: int, y: int, new_count: int):
    # new_count is number of newly inserted places this pass
    conn.execute(
        "UPDATE tiles SET passes=passes+1, new_last_pass=?, total=total+?, consecutive_zero=CASE WHEN ?=0 THEN consecutive_zero+1 ELSE 0 END WHERE search_term=? AND z=? AND x=? AND y=?",
        (new_count, new_count, new_count, search_term, z, x, y),
    )


def tile_should_converge(
    search_term: str, z: int, x: int, y: int, zero_threshold: int = 2
) -> bool:
    row = conn.execute(
        "SELECT consecutive_zero, passes FROM tiles WHERE search_term=? AND z=? AND x=? AND y=?",
        (search_term, z, x, y),
    ).fetchone()
    if not row:
        return False
    consecutive_zero, passes = row
    # adaptive threshold: ceil(1 + passes/3) capped at 6
    dynamic_threshold = int(math.ceil(1 + passes / 3))
    if dynamic_threshold > 6:
        dynamic_threshold = 6
    return consecutive_zero >= dynamic_threshold and passes > 0


async def run_tile_coverage(
    tab,
    search_term: str,
    zoom: int,
    root_zoom: int,
    *,
    max_iters: int = 5,
) -> tuple[int, int, bool, bool, int]:
    # loop calling settle_map_state until saturated or end or iteration cap
    before_total = conn.execute(
        "SELECT COUNT(*) FROM places WHERE search_term=?", (search_term,)
    ).fetchone()[0]
    cumulative_new = 0
    last_count = 0
    last_saturated = False
    feed_complete = False
    iters = 0
    for i in range(max_iters):
        iters = i + 1
        st_count = await settle_map_state(tab, search_term, target_zoom=zoom)
        st, count_s = st_count.split("|")
        last_count = int(count_s)
        # right now, my root zoom is 3z, so I don't know how this would work with 2z or 1z
        # but when I'm at 3z, I don't always see 120 results even if I know it's saturated.
        # maybe it turns out that for some reason, this issue only occurs at the lowest
        # possible zoom on the device. but to play it safe, I'm going to say that at 3z or
        # root zoom below, any nonzero count means saturated.

        if zoom <= 3 or zoom <= root_zoom:
            last_saturated = last_count > 0
        else:
            last_saturated = last_count == 20 or last_count == 61 or last_count >= 120
        after_total = conn.execute(
            "SELECT COUNT(*) FROM places WHERE search_term=?", (search_term,)
        ).fetchone()[0]
        cumulative_new = after_total - before_total
        if st == "end" and (last_count > 0 or iters >= 2):
            feed_complete = True
            break
        await asyncio.sleep(random.uniform(0.4, 0.6))
    return cumulative_new, after_total, last_count, last_saturated, feed_complete, iters


def subdivide_tile(search_term: str, z: int, x: int, y: int) -> int:
    child_z = z + 1
    if child_z > 30:
        return 0
    base_x = x * 2
    base_y = y * 2
    child_rows: list[tuple[str, int, int, int, str]] = []
    for dx in (0, 1):
        for dy in (0, 1):
            cx = base_x + dx
            cy = base_y + dy
            exists = conn.execute(
                "SELECT 1 FROM tiles WHERE search_term=? AND z=? AND x=? AND y=? LIMIT 1",
                (search_term, child_z, cx, cy),
            ).fetchone()
            if exists:
                continue
            child_rows.append((search_term, child_z, cx, cy, "pending"))
    if child_rows:
        conn.executemany(
            "INSERT INTO tiles (search_term, z, x, y, status) VALUES (?, ?, ?, ?, ?)",
            child_rows,
        )
    conn.execute(
        "UPDATE tiles SET expanded=TRUE WHERE search_term=? AND z=? AND x=? AND y=?",
        (search_term, z, x, y),
    )
    return len(child_rows)


def compute_edge_density(
    points: list[tuple[float, float]],
    bounds: tuple[float, float, float, float],
    edge_frac: float = 0.10,
) -> float:
    # fraction of points within edge_frac of any tile boundary (placeholder logic)
    if not points:
        return 0.0
    min_lat, max_lat, min_lon, max_lon = bounds
    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon
    if lat_span <= 0 or lon_span <= 0:
        return 0.0
    edge_lat_margin = lat_span * edge_frac
    edge_lon_margin = lon_span * edge_frac
    edge_count = 0
    for lat, lon in points:
        if (
            lat <= min_lat + edge_lat_margin
            or lat >= max_lat - edge_lat_margin
            or lon <= min_lon + edge_lon_margin
            or lon >= max_lon - edge_lon_margin
        ):
            edge_count += 1
    return edge_count / len(points)


def _mercator_project(lat: float, lon: float, zoom: float) -> tuple[float, float]:
    lat = _clamp(lat, -MAX_LAT, MAX_LAT)
    scale = TILE_SIZE * math.pow(2.0, zoom)
    x = (lon + 180.0) / 360.0 * scale
    sin_lat = math.sin(math.radians(lat))
    y = (0.5 - math.log((1 + sin_lat) / (1 - sin_lat)) / (4 * math.pi)) * scale
    return x, y


def _world_delta(
    cur_lat: float, cur_lon: float, tgt_lat: float, tgt_lon: float, zoom: float
) -> tuple[float, float]:
    if any(v is None for v in (cur_lat, cur_lon, tgt_lat, tgt_lon, zoom)):
        return (0.0, 0.0)
    scale = TILE_SIZE * math.pow(2.0, zoom)
    cur_x, cur_y = _mercator_project(cur_lat, cur_lon, zoom)
    tgt_x, tgt_y = _mercator_project(tgt_lat, tgt_lon, zoom)
    delta_x = tgt_x - cur_x
    if delta_x > scale / 2:
        delta_x -= scale
    elif delta_x < -scale / 2:
        delta_x += scale
    delta_y = tgt_y - cur_y
    return delta_x, delta_y


class Viewport:
    def __init__(self, data: dict):
        self.innerWidth: float = data["innerWidth"]
        self.innerHeight: float = data["innerHeight"]
        self.feedRight: float = data["feedRight"]
        self.mapWidth: float = data["mapWidth"]
        self.mapHeight: float = data["mapHeight"]
        self.mapLeft: float = data["feedRight"]
        self.mapRight: float = data["feedRight"] + data["mapWidth"]
        self.mapCenterX: float = data["mapCenterX"]
        self.mapCenterY: float = data["mapCenterY"]
        self.mapCenter: list[float] = [data["mapCenterX"], data["mapCenterY"]]
        self.devicePixelRatio: float = data["devicePixelRatio"]


async def get_viewport(tab, *, max_age: float = 2.0) -> Viewport:
    loop = asyncio.get_event_loop()
    now = loop.time()
    cached = _VIEWPORT_CACHE["data"]
    if cached and now - _VIEWPORT_CACHE["ts"] < max_age:
        return Viewport(cached)
    js = "(function(){const feed=document.querySelector('[role=feed]');const rect=feed?feed.getBoundingClientRect():{right:0};const feedRight=Math.max(rect.right,0);const mapWidth=Math.max(innerWidth-feedRight,1);const mapHeight=Math.max(innerHeight,1);const mapCenterX=feedRight+mapWidth/2;const mapCenterY=innerHeight/2;return JSON.stringify({innerWidth,innerHeight,feedRight,mapWidth,mapHeight,mapCenterX,mapCenterY,devicePixelRatio:window.devicePixelRatio||1});})()"
    res = await tab.evaluate(js)
    if isinstance(res, cdp.runtime.ExceptionDetails):
        logging.info(js)
        logging.info(res)
        raise RuntimeError("failed to compute viewport metrics")
    if not res:
        raise RuntimeError("viewport metrics evaluation returned empty result")
    data = json.loads(res)
    _VIEWPORT_CACHE["ts"] = now
    _VIEWPORT_CACHE["data"] = data
    return Viewport(data)


async def pan_to(
    tab,
    target_lat: float,
    target_lon: float,
    *,
    target_zoom: float | None = None,
    tolerance_deg: float | None = None,
) -> bool:
    # cost-based adaptive pan routine choosing between zoom vs drag each loop.
    # augmented with zoom-scaled latitude clamp detection feeding back into cost.
    def _zoom_tol(z: float | None) -> float:
        if z is None:
            return PAN_BASE_DEG
        raw = PAN_BASE_DEG * math.exp(-PAN_EXP_DECAY * (z - 1))
        return max(PAN_MIN_DEG, min(raw, PAN_BASE_DEG))

    def _tile_width_deg(z: float) -> float:
        return 360.0 / (2**z)

    def _deg_distance(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
        d_lat = abs(a_lat - b_lat)
        d_lon = abs(_normalize_longitude(a_lon) - _normalize_longitude(b_lon))
        return max(d_lat, d_lon)

    def _pan_step_limit_pixels(view: Viewport) -> float:
        return max(140.0, min((view.mapWidth + view.mapHeight) * 0.28, 650.0))

    current_zoom = lat_lon_zoom(tab.url)[2]
    cur_lat, cur_lon = await get_lat_lon_from_M6(tab)
    if cur_lat is None or cur_lon is None:
        return False
    dist_initial = _deg_distance(cur_lat, cur_lon, target_lat, target_lon)
    # tolerance baseline
    base_tol = _zoom_tol(current_zoom) if tolerance_deg is None else tolerance_deg
    # compute fixed zoom band based on initial distance
    FIT_F = 0.65
    z_fit = math.log2(360.0 / (dist_initial / FIT_F))
    z_fit = max(2.0, min(19.0, z_fit))
    z_lower = int(max(2, math.floor(z_fit - 0.25)))
    # derive upper band by ensuring tolerance spans at least PIX_TOL px at initial zoom
    PIX_TOL = 12.0

    def _pixels_for_tol_fixed(z_test: int) -> float:
        # use initial position for pixel calc
        a_y = _mercator_project(cur_lat, cur_lon, z_test)[1]
        b_lat = cur_lat + base_tol if target_lat > cur_lat else cur_lat - base_tol
        b_y = _mercator_project(b_lat, cur_lon, z_test)[1]
        return abs(b_y - a_y)

    z_upper = int(round(z_fit))
    while z_upper < 19 and _pixels_for_tol_fixed(z_upper) < PIX_TOL:
        z_upper += 1
    if z_upper < z_lower:
        z_upper = z_lower
    # cap z_upper at target_zoom if provided
    if target_zoom is not None:
        z_upper = min(z_upper, target_zoom)
        z_lower = max(
            z_lower, target_zoom - 1
        )  # allow slight below if needed, but not above
    # cost coefficients
    COST_ZOOM = 1.0  # cost per absolute zoom level change
    COST_DRAG_UNIT = 1.0 / 420.0  # cost per pixel of drag (normalized)
    COST_DRAG_UNIT = 1.0 / 420.0
    COST_ZOOM = 1.0
    COST_CLAMP = 3.5
    BAND_LOW_PEN = 2.2
    BAND_HIGH_PEN = 1.6

    # clamp detection tuning (all angular in degrees)
    CLAMP_DECAY = 0.6  # decay per loop of clamp suspicion score
    CLAMP_ESCALATE_CONSEC = 2  # consecutive clamps at same zoom triggers extra zoom out
    CLAMP_MIN_PX_DESIRED = (
        18  # min projected vertical pixels we consider a meaningful intended move
    )
    CLAMP_MAX_PX_REALIZED = (
        3  # realized vertical pixels below this when intended large -> clamp
    )
    MAX_LOOPS = min(140, 16 + int(dist_initial * 8))
    loops = 0
    last_action = "start"
    final_lat = cur_lat
    final_lon = cur_lon
    stagnation = 0
    prev_err = _deg_distance(cur_lat, cur_lon, target_lat, target_lon)
    prev_lon_err = None
    adaptive_tol = base_tol
    clamp_score = 0.0
    clamp_events_consec = 0
    clamp_last_zoom = int(round(current_zoom))
    # lazy root zoom detection (discover coarse floor only after an ineffective zoom-out)
    root_zoom_detected = False
    root_zoom_floor = 0
    success = False
    # guard window after a clamp-driven escalation so we do not instantly zoom back down
    clamp_escalated_guard = 0

    while True:
        loops += 1
        if loops > MAX_LOOPS:
            logging.info("pan_to: exceeded MAX_LOOPS %d", MAX_LOOPS)
            break
        z_now = lat_lon_zoom(tab.url)[2] or current_zoom
        current_zoom = z_now
        cur_lat, cur_lon = await get_lat_lon_from_M6(tab)
        if cur_lat is None or cur_lon is None:
            logging.info("pan_to: failed to get lat/lon")
            break
        if clamp_escalated_guard > 0:
            clamp_escalated_guard -= 1
        final_lat, final_lon = cur_lat, cur_lon
        lat_err = abs(cur_lat - target_lat)
        lon_err = abs(_normalize_longitude(cur_lon) - _normalize_longitude(target_lon))
        # boundary saturation: if target beyond representable latitude edge treat as satisfied vertically
        if abs(target_lat) >= (MAX_LAT - LAT_EDGE_PADDING * 0.5):
            # clamp target inside representable range for error evaluation
            eff_target_lat = _clamp(
                target_lat, -MAX_LAT + LAT_EDGE_PADDING, MAX_LAT - LAT_EDGE_PADDING
            )
            lat_err = abs(cur_lat - eff_target_lat)
            logging.info(
                "pan_to: boundary saturation applied, eff_target_lat=%.7f",
                eff_target_lat,
            )
        err = max(lat_err, lon_err)
        # dynamic stall detect to relax tolerance slightly
        if err > prev_err * 0.995:
            stagnation += 1
        else:
            stagnation = 0
        prev_err = err
        if stagnation >= 3:
            adaptive_tol = min(PAN_BASE_DEG, adaptive_tol * 1.18)
            stagnation = 0
        eff_tol = min(adaptive_tol, _zoom_tol(z_now))
        lon_tile_tol = _tile_width_deg(z_now) / 64.0
        lon_eff_tol = max(lon_tile_tol, eff_tol * 0.5)
        logging.info(
            "pan_to loop %d: lat=%.7f lon=%.7f tgt_lat=%.7f tgt_lon=%.7f z=%.1f lat_err=%.5f lon_err=%.5f eff_tol=%.5f clamp_score=%.3f",
            loops,
            cur_lat,
            cur_lon,
            target_lat,
            target_lon,
            z_now,
            lat_err,
            lon_err,
            eff_tol,
            clamp_score,
        )
        if lat_err <= eff_tol and lon_err <= lon_eff_tol:
            logging.info("pan_to: converged")
            break
        # decay clamp suspicion
        clamp_score *= CLAMP_DECAY

        # apply root zoom floor only after empirical detection
        if root_zoom_detected and z_lower < root_zoom_floor:
            z_lower = root_zoom_floor
        # normalize band each loop; prevent inversion and shrink once within tighter error
        if z_upper < z_lower:
            z_upper = z_lower
        # if root floor detected lock upper to floor when no explicit target
        if root_zoom_detected and target_zoom is None:
            z_upper = root_zoom_floor
        # opportunistic tightening: once error shrinks substantially, avoid widening band
        if prev_err < dist_initial * 0.6 and (z_upper - z_lower) > 2:
            z_upper = max(z_lower + 1, z_lower + (z_upper - z_lower) // 2)
        candidates = {int(round(z_now)), z_lower, z_upper}
        # always allow a test zoom-out; detection will mark floor if ineffective
        if z_now - 1 >= 2:  # hard lower bound from tile scheme
            candidates.add(int(round(z_now - 1)))
        if z_now < 19:
            candidates.add(int(round(z_now + 1)))
        # clamp escape: if clamp persists and z_now < z_upper add z_now+1 strongly
        FORCE_CLAMP_UP = False
        if clamp_events_consec >= 2 and z_now < z_upper:
            FORCE_CLAMP_UP = True
            candidates.add(int(round(z_now + 1)))
        # evaluate cost
        best_zoom = int(round(z_now))
        best_cost = float("inf")
        stay_zoom = int(round(z_now))
        stay_cost = float("inf")
        for zc in candidates:
            zc = int(max(2, min(19, zc)))
            if root_zoom_detected and zc < root_zoom_floor:
                continue
            if target_zoom is not None and zc > target_zoom:
                continue
            # skip immediate zoom-down right after clamp escalation to let higher zoom help drag
            if clamp_escalated_guard > 0 and zc < int(round(z_now)):
                continue
            delta_x, delta_y = _world_delta(
                cur_lat, cur_lon, target_lat, target_lon, zc
            )
            drag_pixels = math.sqrt(delta_x * delta_x + delta_y * delta_y)
            drag_cost = COST_DRAG_UNIT * drag_pixels * max(0.5, zc / 10.0)
            zoom_cost = COST_ZOOM * abs(zc - z_now)
            if zc < z_now:
                zoom_cost *= 0.25  # favor zoom-out
            # band penalties
            band_pen = 0.0
            if zc < z_lower:
                band_pen += BAND_LOW_PEN * (z_lower - zc)
            if zc > z_upper:
                band_pen += BAND_HIGH_PEN * (zc - z_upper)
                if target_zoom is not None and target_zoom < z_now:
                    band_pen += BAND_HIGH_PEN * (
                        zc - z_upper
                    )  # extra penalty for high zoom when target is low
            clamp_pen = 0.0
            if clamp_score > 0.05 and zc == stay_zoom and zc < z_upper:
                # penalize staying coarse under clamp
                clamp_pen += COST_CLAMP * clamp_score
            if clamp_score > 0.05 and zc > z_now:
                clamp_pen += COST_CLAMP * 0.5 * clamp_score
            mismatch_pen = (
                0.1 * abs(zc - target_zoom) if target_zoom is not None else 0.0
            )
            total = drag_cost + zoom_cost + band_pen + clamp_pen + mismatch_pen
            if zc == stay_zoom:
                stay_cost = total
            logging.info(
                "pan_to: zc=%d cost=%.3f drag=%.3f zoom=%.3f band=%.3f clamp=%.3f mismatch=%.3f z_lower=%d z_upper=%d clamp_consec=%d root_detected=%s root_floor=%d",
                zc,
                total,
                drag_cost,
                zoom_cost,
                band_pen,
                clamp_pen,
                mismatch_pen,
                z_lower,
                z_upper,
                clamp_events_consec,
                root_zoom_detected,
                root_zoom_floor,
            )
            if total < best_cost:
                best_cost = total
                best_zoom = zc
        # forced clamp zoom-in override
        if FORCE_CLAMP_UP and best_zoom == stay_zoom and stay_zoom < z_upper:
            best_zoom = stay_zoom + 1
        if best_zoom != stay_zoom and best_cost < stay_cost + 1e-6:
            before_zoom = int(round(z_now))
            logging.info(
                "pan_to: choosing zoom -> %d from %d (band %d-%d root_detected=%s floor=%d)",
                best_zoom,
                before_zoom,
                z_lower,
                z_upper,
                root_zoom_detected,
                root_zoom_floor,
            )
            # stay away from no-feed when there's no search term
            await settle_map_state(
                tab, ignore=["refresh", "scroll", "no-feed"], target_zoom=best_zoom
            )
            after_zoom = lat_lon_zoom(tab.url)[2] or before_zoom
            logging.info(
                "pan_to: zoom attempt complete target=%d before=%d after=%d root_detected=%s floor=%d",
                best_zoom,
                before_zoom,
                after_zoom,
                root_zoom_detected,
                root_zoom_floor,
            )
            # broaden detection: any downward attempt that failed OR overshot upward differently signals floor
            if (
                not root_zoom_detected
                and best_zoom < before_zoom
                and after_zoom == before_zoom
            ):
                root_zoom_detected = True
                root_zoom_floor = before_zoom
                z_upper = root_zoom_floor
                logging.info(
                    "pan_to: root zoom detected empirically at %d", root_zoom_floor
                )
            last_action = f"zoom->{best_zoom}"
            continue
        logging.info(
            "pan_to: choosing drag (band %d-%d best=%d)", z_lower, z_upper, best_zoom
        )
        # perform a drag step at current zoom
        viewport = await get_viewport(tab, max_age=0.0)
        step_limit = _pan_step_limit_pixels(viewport)
        dx, dy = _world_delta(cur_lat, cur_lon, target_lat, target_lon, z_now)
        if abs(dx) <= 1 and abs(dy) <= 1:
            # escalate only if latitude still large and we can gain resolution (z below upper band)
            if (lat_err > eff_tol or lon_err > lon_eff_tol) and int(
                round(z_now)
            ) < z_upper:
                # stay away from no-feed when there's no search term
                await settle_map_state(
                    tab, ignore=["refresh", "scroll", "no-feed"], target_zoom=int(round(z_now + 1))
                )
                last_action = "zoom-escalate"
                continue
            break
        # before drag snapshot for clamp detection (angular + projected pixel intent)
        pre_drag_lat = cur_lat
        pre_drag_merc_y = _mercator_project(cur_lat, cur_lon, z_now)[1]
        intended_merc_y = _mercator_project(target_lat, target_lon, z_now)[1]
        intended_pix_delta_y = intended_merc_y - pre_drag_merc_y
        logging.info(
            "pan_to: pre-drag intended_pix_delta_y=%.1f dx=%.1f dy=%.1f",
            intended_pix_delta_y,
            dx,
            dy,
        )
        center_x, center_y = viewport.mapCenter
        map_left = viewport.mapLeft
        map_right = viewport.mapRight
        top_bound = 80.0
        bottom_bound = viewport.innerHeight - 80.0
        start_x = _clamp(
            center_x + random.uniform(-25, 25), map_left + 40.0, map_right - 60.0
        )
        start_y = _clamp(center_y + random.uniform(-25, 25), top_bound, bottom_bound)
        drag_dx = _clamp(-dx, -step_limit, step_limit)
        drag_dy = _clamp(-dy, -step_limit, step_limit)
        end_x = _clamp(start_x + drag_dx, map_left + 40.0, map_right - 60.0)
        end_y = _clamp(start_y + drag_dy, top_bound, bottom_bound)
        # same here
        await settle_map_state(tab, ignore=["refresh", "scroll", "no-feed"], target_zoom=None)
        await drag_bezier(
            tab,
            x0=start_x,
            y0=start_y,
            x1=end_x,
            y1=end_y,
            steps=int(random.uniform(11, 15)),
            duration=random.uniform(0.38, 0.7),
            bulge=random.uniform(5, 22),
        )
        await asyncio.sleep(random.uniform(0.5, 0.9))
        # post drag clamp detection
        post_lat, post_lon = await get_lat_lon_from_M6(tab)
        if post_lat is not None:
            actual_merc_y = _mercator_project(
                post_lat, post_lon if post_lon is not None else cur_lon, z_now
            )[1]
            actual_pix_delta_y = actual_merc_y - pre_drag_merc_y
            intended_px = abs(intended_pix_delta_y)
            actual_px = abs(actual_pix_delta_y)
            # pixel-based clamp detection tied to zoom (pixels already scaled by zoom in mercator_project)
            # require intended move above threshold and realized movement tiny
            is_clamp = (
                intended_px >= CLAMP_MIN_PX_DESIRED
                and actual_px <= CLAMP_MAX_PX_REALIZED
            )
            logging.info(
                "pan_to: post-drag post_lat=%.7f actual_pix_delta_y=%.1f intended_px=%.1f actual_px=%.1f is_clamp=%s",
                post_lat,
                actual_pix_delta_y,
                intended_px,
                actual_px,
                is_clamp,
            )
            if is_clamp:
                clamp_score = min(1.5, clamp_score + 1.0)
                if int(round(z_now)) == clamp_last_zoom:
                    clamp_events_consec += 1
                else:
                    clamp_events_consec = 1
                    clamp_last_zoom = int(round(z_now))
                logging.info(
                    "pan_to: clamp detected, score=%.3f consec=%d",
                    clamp_score,
                    clamp_events_consec,
                )
                if clamp_events_consec >= CLAMP_ESCALATE_CONSEC:
                    # suppress escalation only if substantial horizontal improvement and latitude error not hugely above tolerance
                    if prev_lon_err is None:
                        horiz_progress = False
                    else:
                        horiz_progress = lon_err < prev_lon_err * 0.7 and lat_err <= eff_tol * 2
                    zoom_target = None
                    if not horiz_progress:
                        if target_zoom is not None and z_now > target_zoom:
                            zoom_target = max(
                                root_zoom_floor if root_zoom_detected else 2,
                                int(round(z_now - 1)),
                            )
                            action = "zoom-out-clamp"
                        else:
                            zoom_target = min(19, int(round(z_now + 1)))
                            if target_zoom is not None:
                                zoom_target = min(zoom_target, target_zoom)
                            # do not zoom above root floor lock
                            if root_zoom_detected and zoom_target > root_zoom_floor:
                                zoom_target = root_zoom_floor
                            action = "zoom-in-clamp"
                        # expand band upward so cost model does not immediately force zoom-out
                        if zoom_target > z_upper:
                            z_upper = zoom_target
                        logging.info("pan_to: triggering %s to %d", action, zoom_target)
                    else:
                        logging.info("pan_to: suppressing clamp escalation due to horiz progress")
                    if zoom_target is not None and zoom_target == int(round(z_now)):
                        logging.info(
                            "pan_to: skipping escalation at max zoom %d", zoom_target
                        )
                        if clamp_events_consec >= 2:
                            # if we are pretty close to the edge, then still succeed rather than quitting
                            max_err = _zoom_tol(z_now) * (2 + (await get_viewport(tab)).devicePixelRatio)
                            success = lat_err <= max_err
                            logging.info(
                                "pan_to is_clamp: success=%s | lat_err=%.5f <= max_err=%.5f",
                                success,
                                lat_err,
                                max_err,
                            )
                            break
                    elif zoom_target is not None:
                        before_zoom = int(round(z_now))
                        await settle_map_state(
                            tab, ignore=["refresh", "scroll", "no-feed"], target_zoom=zoom_target
                        )
                        after_zoom = lat_lon_zoom(tab.url)[2] or before_zoom
                        if (
                            zoom_target < before_zoom
                            and after_zoom == before_zoom
                            and not root_zoom_detected
                        ):
                            root_zoom_detected = True
                            root_zoom_floor = before_zoom
                            logging.info(
                                "pan_to: root zoom detected during %s at %d",
                                action,
                                root_zoom_floor,
                            )
                        # set guard to hold higher zoom for a couple loops
                        if zoom_target > before_zoom:
                            clamp_escalated_guard = 2
                        last_action = action
                        clamp_events_consec = 0
                        continue
            else:
                clamp_events_consec = 0
        last_action = "drag"
    prev_lon_err = lon_err
    # final success evaluation
    zoom_final = lat_lon_zoom(tab.url)[2]
    tol_final = _zoom_tol(zoom_final)
    if final_lat is not None and final_lon is not None and not success:
        lat_err = abs(final_lat - target_lat)
        lon_err = abs(
            _normalize_longitude(final_lon) - _normalize_longitude(target_lon)
        )
        lon_tile_tol = _tile_width_deg(zoom_final if zoom_final else 1) / 64.0
        success = lat_err <= tol_final and lon_err <= max(lon_tile_tol, tol_final * 0.5)
    logging.info(
        "pan_to: final lat_err=%.5f lon_err=%.5f tol=%.5f success=%s loops=%d",
        lat_err,
        lon_err,
        tol_final,
        success,
        loops,
    )
    if not success:
        logging.warning(
            "pan_to incomplete @%.7f,%.7f,%sz lat_err=%.5f lon_err=%.5f tol=%.5f loops=%d last=%s",
            final_lat,
            final_lon,
            zoom_final,
            lat_err,
            lon_err,
            tol_final,
            loops,
            last_action,
        )
    await settle_map_state(tab, ignore=["refresh", "scroll", "no-feed"], target_zoom=target_zoom)
    return success


async def hit_key(
    tab,
    modifiers: int | None = None,
    timestamp: cdp.input_.TimeSinceEpoch | None = None,
    text: str | None = None,
    unmodified_text: str | None = None,
    key_identifier: str | None = None,
    code: str | None = None,
    key: str | None = None,
    windows_virtual_key_code: int | None = None,
    native_virtual_key_code: int | None = None,
    auto_repeat: bool | None = None,
    is_keypad: bool | None = None,
    is_system_key: bool | None = None,
    location: int | None = None,
    commands: list[str] | None = None,
):
    # build kwargs dict for dispatch_key_event
    kwargs = dict(
        key=key,
        code=code,
        windows_virtual_key_code=windows_virtual_key_code,
        native_virtual_key_code=native_virtual_key_code,
        modifiers=modifiers,
        timestamp=timestamp,
        text=text,
        unmodified_text=unmodified_text,
        key_identifier=key_identifier,
        auto_repeat=auto_repeat,
        is_keypad=is_keypad,
        is_system_key=is_system_key,
        location=location,
        commands=commands,
    )
    # remove None values
    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    await tab.send(
        cdp.input_.dispatch_key_event(
            type_="keyDown",
            **kwargs,
        )
    )
    await tab.send(
        cdp.input_.dispatch_key_event(
            type_="keyUp",
            **kwargs,
        )
    )


async def hit_enter(tab):
    await hit_key(
        tab,
        key="Enter",
        code="Enter",
        windows_virtual_key_code=13,
        native_virtual_key_code=13,
    )


async def hit_backspace(tab):
    await hit_key(
        tab,
        key="Backspace",
        code="Backspace",
        windows_virtual_key_code=8,
        native_virtual_key_code=8,
    )


def lat_lon_zoom(url):
    # extract lat, lon, zoom from any '@lat,lon,zoomz' pattern in the url
    match = re.search(
        r"@(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)z",
        url,
    )
    if not match:
        logging.info("could not parse lat/lon/zoom from url: %s", url)
        return [None, None, None]
    return [float(d) for d in match.groups()]


async def get_lat_lon_from_M6(tab):
    for _ in range(3):
        try:
            # wait for _.M6 evaluation with timeout
            m6_raw = await asyncio.wait_for(
                tab.evaluate("[..._.M6].join(',')"), timeout=0.5
            )
            if isinstance(m6_raw, cdp.runtime.ExceptionDetails):
                if _ >= 2:
                    m6 = lat_lon_zoom(tab.url)
                    return m6[0], m6[1]
            else:
                break
        except asyncio.TimeoutError:
            if _ >= 2:
                logging.info("_.M6 evaluation timed out")
                m6 = lat_lon_zoom(tab.url)
                return m6[0], m6[1]
    m6 = m6_raw.split(",")
    return float(m6[1]), float(m6[0])


async def get_current_state(tab):
    with open("current_state.js", "r") as f:
        js = f.read()
    state = await tab.evaluate(js)
    if isinstance(state, cdp.runtime.ExceptionDetails):
        raise Exception(state)
    return state


async def _align_zoom_to_target(tab, target_zoom: int, *, tolerance: float = 0.05):
    zoom = lat_lon_zoom(tab.url)[2]
    if zoom is None:
        return
    center = await get_map_viewport_center(tab)
    zoom_crossings = 0
    did_adjust_zoom = False
    no_change_count = 0
    while zoom is not None and abs(zoom - target_zoom) > tolerance:
        did_adjust_zoom = True
        direction_down = zoom > target_zoom
        await settle_map_state(tab, ignore=["refresh", "scroll", "no-feed"])
        zoom_before = lat_lon_zoom(tab.url)[2]
        await scroll(
            tab,
            x=center[0],
            y=center[1],
            scroll_down=direction_down,
            steps=1,
        )
        await asyncio.sleep(random.uniform(0.5, 0.6))
        next_zoom = lat_lon_zoom(tab.url)[2]
        # if zoom did not change, wait a bit longer before next attempt
        if next_zoom == zoom_before:
            await asyncio.sleep(random.uniform(0.3, 0.4))
            no_change_count += 1
            # detect root zoom floor: if trying to zoom out and zoom won't change after 3 attempts
            if direction_down and no_change_count >= 3:
                logging.info(
                    "_align_zoom_to_target: root zoom floor detected at %d (target %d)",
                    zoom_before,
                    target_zoom,
                )
                zoom = zoom_before
                break
        else:
            no_change_count = 0
        if next_zoom is None:
            break
        if (zoom < target_zoom and next_zoom > target_zoom) or (
            zoom > target_zoom and next_zoom < target_zoom
        ):
            zoom_crossings += 1
        if zoom_crossings > 2:
            logging.info("too many zoom crossings, forcing normalization")
            for _ in range(12):
                await settle_map_state(tab, ignore=["refresh", "scroll", "no-feed"])
                await scroll(
                    tab,
                    x=center[0],
                    y=center[1],
                    steps=7,
                    step_duration=0.01,
                    scroll_down=True,
                )
                await asyncio.sleep(random.uniform(0.05, 0.15))
            zoom = lat_lon_zoom(tab.url)[2]
            if zoom is None:
                break
            for _ in range(target_zoom - zoom):
                await settle_map_state(tab, ignore=["refresh", "scroll", "no-feed"])
                await scroll(
                    tab,
                    x=center[0],
                    y=center[1],
                    steps=4,
                    step_duration=0.01,
                    scroll_down=False,
                )
                await asyncio.sleep(random.uniform(0.05, 0.15))
                zoom = lat_lon_zoom(tab.url)[2]
                if zoom is None:
                    break
            zoom_crossings = 0
            continue
        zoom = lat_lon_zoom(tab.url)[2]


async def settle_map_state(
    tab,
    search_term: str = None,
    feed_coords=None,
    ignore: list[str] = [],
    last_loading: datetime = None,
    target_zoom: int = None,
):
    local_feed_coords = feed_coords
    local_last_loading = last_loading

    while True:
        if target_zoom is not None:
            await _align_zoom_to_target(tab, target_zoom)
            await settle_map_state(tab, search_term, ignore=ignore)
        st = await get_current_state(tab)
        state_key = st.split("|")[0]
        if state_key in ignore:
            return st
        logging.debug("map state: %s", st)
        if state_key == "end":
            viewport = await get_viewport(tab, max_age=0.0)
            await tab.mouse_move(
                x=random.uniform(viewport.mapLeft + 1, viewport.mapRight - 1),
                y=random.uniform(1, viewport.innerHeight - 1),
                steps=1,
            )
            return st
        if state_key not in ("loading", "no-feed"):
            local_last_loading = None

        if st == "popup":
            dismiss_coords = await wait_for_selector_center(
                tab, "#popup button[aria-label=Dismiss]"
            )
            await asyncio.sleep(random.uniform(0.5, 0.9))
            await tab.mouse_click(
                x=dismiss_coords[0] + random.uniform(-5, 5),
                y=dismiss_coords[1] + random.uniform(-5, 5),
            )
            await asyncio.sleep(random.uniform(0.4, 0.7))
            continue
        if st == "scroll":
            if local_feed_coords is None:
                local_feed_coords = [
                    (
                        v + random.uniform(-50, 50)
                        if i == 0
                        else v + random.uniform(-75, 75)
                    )
                    for i, v in enumerate(
                        await wait_for_selector_center(tab, "div[role=feed]")
                    )
                ]
            await scroll(
                tab,
                x=local_feed_coords[0],
                y=local_feed_coords[1],
                steps=int(random.uniform(6, 8)),
                step_duration=random.uniform(0.04, 0.09),
            )
            await asyncio.sleep(random.uniform(0.2, 0.4))
            continue
        if st == "hovercard":
            hc_dim = await get_dimensions(tab, "#interactive-hovercard")
            await tab.mouse_move(
                x=hc_dim["right"] + random.uniform(-50, 5),
                y=hc_dim["top"] - random.uniform(5, 25),
                steps=1,
            )
            continue
        if st == "refresh":
            refresh_coords = await wait_for_selector_center(
                tab, 'button[jsaction="search.refresh"]'
            )
            if refresh_coords != [0.0, 0.0]:
                await tab.mouse_move(
                    x=refresh_coords[0] + random.uniform(-5, 5),
                    y=random.uniform(5, refresh_coords[1]),
                    steps=1,
                )
                await tab.mouse_click(x=refresh_coords[0], y=refresh_coords[1])
                await asyncio.sleep(random.uniform(0.55, 0.7))
            continue
        if st == "place-open":
            try:
                close_coords = await wait_for_selector_center(
                    tab, "[role=main][aria-label] button[aria-label=Close]", 0
                )
            except asyncio.TimeoutError:
                try:
                    close_coords = await wait_for_selector_center(
                        tab, "[role=search] button[aria-label=Back]", 0
                    )
                except asyncio.TimeoutError:
                    close_coords = None
            if close_coords:
                await tab.mouse_click(
                    x=close_coords[0] + random.uniform(-10, 10),
                    y=close_coords[1] + random.uniform(-10, 10),
                )
                await asyncio.sleep(random.uniform(0.6, 1.1))
            continue
        if st == "survey":
            decline = await tab.evaluate(
                "s=document.getElementById('google-hats-survey');bb=s?.contentDocument?.querySelector('.scSurveyInvitationsurveycomponentdecline-button button')?.getBoundingClientRect();sb=s?.getBoundingClientRect();[sb?.left+bb?.left+bb?.width/2||0,sb?.top+bb?.top+bb?.height/2||0].join(',')"
            )
            if isinstance(decline, cdp.runtime.ExceptionDetails):
                raise Exception(decline)
            decline_coords = [float(v) for v in decline.split(",")]
            await tab.mouse_click(
                x=decline_coords[0] + random.uniform(-15, 15),
                y=decline_coords[1] + random.uniform(-8, 8),
            )
            await asyncio.sleep(random.uniform(0.5, 0.9))
            continue
        if st == "loading":
            if local_last_loading is None:
                local_last_loading = datetime.now(UTC)
            elif datetime.now(UTC) - local_last_loading > timedelta(seconds=7):
                logging.info("loading state timeout, reloading page")
                await tab.reload()
                await nodriverplus.wait_for_page_load(tab)
                await apply_debug(tab)
                local_last_loading = None
                continue
            await asyncio.sleep(random.uniform(0.5, 0.8))
            continue
        if st == "no-feed":
            sbi_coords = await wait_for_selector_center(tab, "#searchboxinput", 2000)
            sbi = await tab.select("#searchboxinput")
            await tab.mouse_click(
                x=sbi_coords[0] + random.uniform(-10, 10),
                y=sbi_coords[1] + random.uniform(-10, 10),
            )
            await asyncio.sleep(random.uniform(0.3, 0.6))
            if await get_current_state(tab) != "no-feed":
                sb_dim = await get_dimensions(tab, "#searchbox")
                await tab.mouse_click(
                    x=sb_dim["left"] + sb_dim["width"] / 2 + random.uniform(-20, 20),
                    y=random.uniform(0, sb_dim["top"] - 1),
                )
                continue
            if search_term:
                existing_value = await tab.evaluate(
                    "document.getElementById('searchboxinput').value"
                )
                if (
                    existing_value is None
                    or existing_value.lower() != search_term.lower()
                ):
                    for _ in range(len(existing_value or "")):
                        await hit_backspace(tab)
                        await asyncio.sleep(random.uniform(0.05, 0.1))
                    await asyncio.sleep(random.uniform(0.3, 0.6))
                    for c in search_term:
                        await asyncio.sleep(random.uniform(0.1, 0.2))
                        await sbi.send_keys(c)
                    await asyncio.sleep(random.uniform(0.4, 0.8))
                    await hit_enter(tab)
                    await asyncio.sleep(random.uniform(0.5, 1.0))
            search_button_coords = [
                float(v)
                for v in (
                    await tab.evaluate(
                        "s=[...document.querySelectorAll('[data-suggestion-index]')].map(e=>{let t=[...e.querySelectorAll('[role=gridcell] > span[class]')].map(e=>e.textContent);return{e:e,m:document.querySelector('#searchboxinput').value.toLowerCase()==t[0]?.toLowerCase(),s:'See locations'==t[1]}});e=s.find(e=>e.m&&e.s)?.e||s.find(e=>e.m&&!e.s)?.e;r=e?.getBoundingClientRect()||document.querySelector('#searchbox-searchbutton').getBoundingClientRect();[r.left+r.width/2,r.top+r.height/2].join(',')"
                    )
                ).split(",")
            ]
            await tab.mouse_click(
                x=search_button_coords[0] + random.uniform(-10, 10),
                y=search_button_coords[1] + random.uniform(-10, 10),
            )
            await asyncio.sleep(random.uniform(0.3, 0.6))
            continue
        await asyncio.sleep(random.uniform(0.2, 0.4))


async def apply_debug(tab):
    with open("debug.js") as f:
        await tab.evaluate(f.read())


async def main():
    ndp = nodriverplus.NodriverPlus(
        solve_cloudflare=False,
        request_paused_handler=MapScraper,
    )
    browser = await ndp.start()
    await browser.main_tab.maximize()

    # load persisted map state if available
    saved_state = conn.execute(
        "SELECT lat, lon, zoom, pan_direction, search_term, last_lat_direction, current_tile_z, current_tile_x, current_tile_y FROM map_state LIMIT 1"
    ).fetchone()
    start_url = "https://maps.google.com"
    search_term = None
    if saved_state and saved_state[0] is not None and saved_state[1] is not None:
        (
            s_lat,
            s_lon,
            s_zoom,
            s_pan,
            s_search,
            s_last_lat_dir,
            s_tile_z,
            s_tile_x,
            s_tile_y,
        ) = saved_state
        if s_zoom is None:
            s_zoom = 12.0
        search_term = s_search
        # construct maps url preserving last lat/lon/zoom (with search if we have it)
        if search_term:
            # simple encoding for spaces
            path_term = search_term.replace(" ", "+")
            start_url = f"https://www.google.com/maps/search/{path_term}/@{s_lat},{s_lon},{s_zoom}z"
        else:
            start_url = f"https://www.google.com/maps/@{s_lat},{s_lon},{s_zoom}z"

    res = await ndp.get_with_timeout(start_url, page_load_timeout=25)
    for _ in range(5):
        if not res.timed_out:
            break
        await res.tab.send(cdp.page.stop_loading())
        await asyncio.sleep(random.uniform(1.5, 2.5))
        res = await ndp.get_with_timeout(start_url, page_load_timeout=25)
    tab = res.tab

    await apply_debug(tab)

    # pick or resume search term
    if not search_term:
        search_term = "ymca"  # default term if none persisted yet
        sb_exists = False
        while not sb_exists:
            # google maps may need reloaded if the search box never appears
            try:
                await wait_for_selector_center(tab, "#searchboxinput")
                await apply_debug(tab)
                sb_exists = True
                await asyncio.sleep(random.uniform(1.0, 2.0))
            except asyncio.TimeoutError:
                logging.info("reloading tab")
                await tab.reload()
                await nodriverplus.wait_for_page_load(tab)

    await settle_map_state(tab, search_term, ignore=["refresh", "scroll"])

    await asyncio.sleep(1 + 1.5 * random.random())

    lat, lon = await get_lat_lon_from_M6(tab)
    zoom = lat_lon_zoom(tab.url)[2]

    existing_tile_rows = conn.execute(
        "SELECT COUNT(*) FROM tiles WHERE search_term = ?", (search_term,)
    ).fetchone()[0]
    await asyncio.sleep(random.uniform(3, 4))
    map_center = await get_map_viewport_center(tab)
    root_zoom = await zoom_out_to_limit(tab, map_center)
    logging.info("world zoom-out complete: root_zoom=%s", root_zoom)
    if existing_tile_rows == 0:
        # world zoom-out and single coarse tile seeding (lazy frontier)
        await asyncio.sleep(random.uniform(0.3, 0.6))
        root_lat, root_lon = await get_lat_lon_from_M6(tab)
        coarse_inserted, coarse_z, _ = ensure_initial_frontier(
            search_term, root_lat, root_lon, root_zoom
        )
        # breadth seeding: insert all tiles at root zoom level to ensure traversal across region
        if coarse_z is not None:
            n = 2**coarse_z
            # only breadth seed if small grid (avoid explosion at high zoom)
            if n <= 16:  # z<=4 gives at most 16x16=256 tiles
                existing_any = conn.execute(
                    "SELECT 1 FROM tiles WHERE search_term=? AND z=? LIMIT 1",
                    (search_term, coarse_z),
                ).fetchone()
                # we already inserted one; now add the rest
                breadth_rows = []
                for tx in range(n):
                    for ty in range(n):
                        if (
                            tx == 0 and ty == 0 and coarse_inserted
                        ):  # already ensured center tile; still verify
                            pass
                        exists_tile = conn.execute(
                            "SELECT 1 FROM tiles WHERE search_term=? AND z=? AND x=? AND y=? LIMIT 1",
                            (search_term, coarse_z, tx, ty),
                        ).fetchone()
                        if exists_tile:
                            continue
                        breadth_rows.append((search_term, coarse_z, tx, ty, "pending"))
                if breadth_rows:
                    conn.executemany(
                        "INSERT INTO tiles (search_term, z, x, y, status) VALUES (?, ?, ?, ?, ?)",
                        breadth_rows,
                    )
                    logging.info(
                        "breadth seeded root grid: z=%d tiles=%d (n=%d)",
                        coarse_z,
                        len(breadth_rows),
                        n,
                    )
        logging.info(
            "initialized frontier (lazy): coarse_z=%s coarse_ins=%d term='%s'",
            coarse_z,
            coarse_inserted,
            search_term,
        )
    else:
        # no world backfill anymore; simply ensure at least one frontier tile exists
        existing_any = conn.execute(
            "SELECT 1 FROM tiles WHERE search_term = ? LIMIT 1", (search_term,)
        ).fetchone()
        if not existing_any:
            inserted, z_used, _ = ensure_initial_frontier(search_term, lat, lon, zoom)
            logging.info(
                "restored run: inserted %d initial frontier tile at z=%s for term '%s'",
                inserted,
                z_used,
                search_term,
            )
        else:
            logging.info(
                "restored run: existing frontier present for term '%s'", search_term
            )

    async def process_tile(z: int, x: int, y: int) -> tuple[int, bool]:
        center_lat, center_lon = mercator_tile_center(z, x, y)
        moved = await pan_to(tab, center_lat, center_lon, target_zoom=z)
        if not moved:
            logging.info("tile_pan_failed z=%d x=%d y=%d", z, x, y)
            return 0, False
        new_delta, total_places, last_count, last_saturated, feed_complete, iters = (
            await run_tile_coverage(tab, search_term, z, root_zoom)
        )
        update_tile_pass_metrics(search_term, z, x, y, new_delta)
        # snapshot fields
        conn.execute(
            "UPDATE tiles SET last_count=?, last_saturated=?, feed_complete=? WHERE search_term=? AND z=? AND x=? AND y=?",
            (
                last_count,
                last_saturated,
                feed_complete,
                search_term,
                z,
                x,
                y,
            ),
        )
        spawned_children = 0
        converged = False
        if last_saturated:
            spawned_children = subdivide_tile(search_term, z, x, y)
            mark_tile_status(search_term, z, x, y, "complete")
        else:
            # convergence based only on zero streak for now (adaptive threshold later)
            converged = feed_complete and tile_should_converge(search_term, z, x, y)
            if converged:
                mark_tile_status(search_term, z, x, y, "complete")
            else:
                mark_tile_status(search_term, z, x, y, "pending")
        logging.info(
            "tile_event @%.7f,%.7f,%fz new=%d total=%d last_count=%d saturated=%s feed_complete=%s converged=%s children=%d iters=%d",
            center_lat,
            center_lon,
            z,
            new_delta,
            total_places,
            last_count,
            last_saturated,
            feed_complete,
            converged,
            spawned_children,
            iters,
        )
        # checkpoint map state after a tile pass loop
        cur_lat, cur_lon = await get_lat_lon_from_M6(tab)
        cur_zoom = lat_lon_zoom(tab.url)[2]
        save_map_state(cur_lat, cur_lon, cur_zoom, "pan", search_term, None)
        return new_delta, last_saturated

    # scheduler loop
    while True:
        row = next_pending_tile(search_term)
        if not row:
            # check for completion
            unfinished = conn.execute(
                "SELECT COUNT(*) FROM tiles WHERE search_term=? AND (status != 'completed' OR expanded = FALSE OR feed_complete = FALSE)",
                (search_term,),
            ).fetchone()[0]
            if unfinished == 0:
                logging.info("All tiles completed for search_term %s", search_term)
                break
            await asyncio.sleep(2.0)
            continue
        z, x, y = row
        mark_tile_status(search_term, z, x, y, "in_progress")
        await process_tile(z, x, y)
        # save current tile
        cur_lat, cur_lon = await get_lat_lon_from_M6(tab)
        cur_zoom = lat_lon_zoom(tab.url)[2]
        save_map_state(cur_lat, cur_lon, cur_zoom, "pan", search_term, None, z, x, y)
        await asyncio.sleep(random.uniform(0.5, 1.2))


async def open_browser():
    ndp = nodriverplus.NodriverPlus(
        request_paused_handler=MapScraper,
    )
    browser = await ndp.start()
    await browser.main_tab.maximize()
    tab = (await ndp.get_with_timeout("https://maps.google.com")).tab
    while True:
        logging.info("current state: %s", await get_current_state(tab))
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
