"""XY plotter control utilities with built-in motion patterns."""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Iterator, Optional, Tuple, Union

import numpy as np

try:
    import serial
except ImportError:  # pragma: no cover - optional for non-hardware workflows
    serial = None

Point = Tuple[float, float]
PatternGenerator = Callable[["WorkArea"], Iterable[Point]]
PatternInput = Union[str, PatternGenerator, None]


@dataclass
class WorkArea:
    """Simple rectangular work envelope."""

    width: float = 1250.0
    height: float = 1250.0
    margin: float = 10.0

    def __post_init__(self) -> None:
        if self.margin * 2 >= self.width or self.margin * 2 >= self.height:
            raise ValueError("Margin leaves no space for movement in the work area")

    @property
    def xmin(self) -> float:
        return self.margin

    @property
    def xmax(self) -> float:
        return self.width - self.margin

    @property
    def ymin(self) -> float:
        return self.margin

    @property
    def ymax(self) -> float:
        return self.height - self.margin

    @property
    def center(self) -> Point:
        return ((self.xmin + self.xmax) / 2, (self.ymin + self.ymax) / 2)

    def clamp(self, x: float, y: float) -> Point:
        """Clamp a point to remain inside the work area."""
        return (
            min(max(x, self.xmin), self.xmax),
            min(max(y, self.ymin), self.ymax),
        )


class XYPlotter:
    """GRBL-based plotter controller for ACRO hardware."""

    def __init__(self, port: str, baudrate: int = 115200, timeout: float = 1.0):
        if serial is None:
            raise ImportError("pyserial is required for XYPlotter; install via `pip install pyserial`.")
        self.ser = serial.Serial(port, baudrate=baudrate, timeout=timeout)
        self._wake_up()

    def __enter__(self) -> "XYPlotter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _wake_up(self) -> None:
        self.ser.write(b"\r\n\r\n")
        time.sleep(2)
        self.ser.flushInput()

    def send_gcode(self, command: str) -> None:
        """Send a raw G-code line."""
        if not command.endswith("\n"):
            command = f"{command}\n"
        self.ser.write(command.encode())

    def wait_till_idle(
        self,
        poll_interval: float = 0.1,
        verbose: bool = False,
        show_position: bool = True,
    ) -> None:
        """Poll GRBL status until the controller reports Idle."""
        while True:
            time.sleep(poll_interval)
            self.ser.flushInput()
            self.ser.write(b"?\n")
            response = self.ser.readline().decode(errors="ignore").strip()
            if verbose and response:
                print(response)
            if show_position and response:
                status_line = _format_status_position(response)
                if status_line:
                    print(status_line, end="\r", flush=True)
            if "Idle" in response:
                if show_position:
                    print(" " * 80, end="\r", flush=True)
                break

    def home(self) -> None:
        """Home the plotter and zero the work coordinate system."""
        self.send_gcode("G54")
        self.send_gcode("?")
        self.wait_till_idle()

        self.send_gcode("$H")
        self.wait_till_idle()

        self.send_gcode("G10 P0 L20 X0 Y0 Z0")
        self.wait_till_idle()

        self.send_gcode("G54")
        self.send_gcode("?")
        self.wait_till_idle()

    def move(self, x: float, y: float, feed_rate: float = 20.0, wait_idle: bool = True) -> None:
        """Rapid move to an XY coordinate."""
        command = f"G0 X{x:.3f} Y{y:.3f} F{feed_rate}\n"
        self.ser.write(command.encode())
        if wait_idle:
            self.wait_till_idle()

    def move_to_origin(self, wait_idle: bool = True) -> None:
        self.move(0, 0, wait_idle=wait_idle)

    def run_pattern(
        self,
        area: WorkArea,
        pattern: PatternInput = None,
        feed_rate: float = 20.0,
        dwell: float = 0.0,
        wait_idle: bool = True,
    ) -> None:
        """Execute a pattern (callable or name) over the provided work area."""
        generator = resolve_pattern(pattern)
        for x, y in generator(area):
            self.move(x, y, feed_rate=feed_rate, wait_idle=wait_idle)
            if dwell > 0:
                time.sleep(dwell)

    def close(self) -> None:
        if self.ser and self.ser.is_open:
            self.ser.close()


def _format_status_position(response: str) -> Optional[str]:
    """Extract a concise status line with position from a GRBL status response."""
    if not response.startswith("<") or not response.endswith(">"):
        return None

    fields = response[1:-1].split("|")
    if not fields:
        return None

    state = fields[0].strip()
    label = None
    position = None
    for field in fields[1:]:
        if field.startswith("WPos:"):
            label = "WPos"
            position = field[5:]
            break
        if field.startswith("MPos:"):
            label = "MPos"
            position = field[5:]
            break

    if not position or not label:
        return None

    coords = [coord.strip() for coord in position.split(",") if coord.strip()]
    formatted = ", ".join(coords[:3]) if coords else position.strip()
    if state:
        return f"{state} {label}: {formatted}"
    return f"{label}: {formatted}"


def serpentine_grid(area: WorkArea, spacing: float) -> Iterator[Point]:
    """Generate a classic zig-zag raster across the work area."""
    if spacing <= 0:
        raise ValueError("Spacing must be positive")

    x_values = np.arange(area.xmin, area.xmax + spacing / 2, spacing)
    y_values = np.arange(area.ymin, area.ymax + spacing / 2, spacing)

    for row_index, y in enumerate(y_values):
        xs = x_values if row_index % 2 == 0 else x_values[::-1]
        for x in xs:
            yield float(x), float(y)


def concentric_square_rings(area: WorkArea, spacing: float = 80.0) -> Iterator[Point]:
    """Walk concentric square perimeters expanding from the center."""
    if spacing <= 0:
        raise ValueError("Spacing must be positive")

    cx, cy = area.center
    max_offset = min(cx - area.xmin, area.xmax - cx, cy - area.ymin, area.ymax - cy)
    offsets = np.arange(0, max_offset + spacing / 2, spacing)

    for offset in offsets:
        left = cx - offset
        right = cx + offset
        bottom = cy - offset
        top = cy + offset

        for x in np.arange(left, right + spacing / 2, spacing):
            yield area.clamp(float(x), float(top))
        for y in np.arange(top - spacing, bottom - spacing / 2, -spacing):
            yield area.clamp(float(right), float(y))
        for x in np.arange(right - spacing, left - spacing / 2, -spacing):
            yield area.clamp(float(x), float(bottom))
        for y in np.arange(bottom + spacing, top + spacing / 2, spacing):
            yield area.clamp(float(left), float(y))


def progressive_raster(
    area: WorkArea,
    initial_spacing: float = 300.0,
    passes: int = 4,
    spacing_decay: float = 0.5,
) -> Iterator[Point]:
    """Run multiple raster scans, getting denser on each pass."""
    if initial_spacing <= 0:
        raise ValueError("Initial spacing must be positive")
    if spacing_decay <= 0:
        raise ValueError("Spacing decay must be positive")
    if passes < 1:
        return

    spacing = initial_spacing
    for _ in range(passes):
        yield from serpentine_grid(area, spacing)
        spacing *= spacing_decay
        if spacing <= 0:
            break


def center_out_refined_spiral(
    area: WorkArea,
    initial_spacing: float = 250.0,
    spacing_decay: float = 0.65,
    min_spacing: float = 35.0,
    angle_step_deg: float = 6.0,
) -> Iterator[Point]:
    """Default pattern: start in the center, spiral out, tightening spacing each turn."""
    if initial_spacing <= 0:
        raise ValueError("Initial spacing must be positive")
    if min_spacing <= 0:
        raise ValueError("Minimum spacing must be positive")
    if angle_step_deg <= 0:
        raise ValueError("Angle step must be positive")

    cx, cy = area.center
    max_radius = max(
        cx - area.xmin,
        area.xmax - cx,
        cy - area.ymin,
        area.ymax - cy,
    )
    spacing = initial_spacing
    radius_offset = 0.0
    angle_step = math.radians(angle_step_deg)
    theta = 0.0
    last_point: Optional[Point] = None

    while radius_offset <= max_radius + spacing:
        radius = radius_offset + (spacing / (2 * math.pi)) * theta
        x = cx + radius * math.cos(theta)
        y = cy + radius * math.sin(theta)
        point = area.clamp(x, y)

        if point != last_point:
            yield point
            last_point = point

        theta += angle_step
        if theta >= 2 * math.pi:
            theta -= 2 * math.pi
            radius_offset += spacing
            spacing = max(min_spacing, spacing * spacing_decay)


def radial_spokes(
    area: WorkArea,
    rays: int = 24,
    radial_step: float = 60.0,
    alternate_direction: bool = True,
) -> Iterator[Point]:
    """Trace repeated rays from the center outward, expanding radius each lap."""
    if rays < 1:
        raise ValueError("Number of rays must be positive")
    if radial_step <= 0:
        raise ValueError("Radial step must be positive")

    cx, cy = area.center
    max_radius = max(
        cx - area.xmin,
        area.xmax - cx,
        cy - area.ymin,
        area.ymax - cy,
    )

    yield cx, cy
    ray_indices = list(range(rays))
    radius = radial_step
    toggle = False

    while radius <= max_radius + radial_step:
        indices = ray_indices if not (alternate_direction and toggle) else ray_indices[::-1]
        for idx in indices:
            angle = (2 * math.pi * idx) / rays
            x = cx + radius * math.cos(angle)
            y = cy + radius * math.sin(angle)
            yield area.clamp(x, y)
        toggle = not toggle
        radius += radial_step


def phyllotaxis_fill(
    area: WorkArea,
    points: int = 500,
    step: float = 22.0,
    angle_deg: float = 137.5,
) -> Iterator[Point]:
    """Golden-angle spiral for even coverage."""
    if points < 1:
        return
    if step <= 0:
        raise ValueError("Step must be positive")
    if angle_deg <= 0:
        raise ValueError("Angle must be positive")

    cx, cy = area.center
    max_radius = min(
        cx - area.xmin,
        area.xmax - cx,
        cy - area.ymin,
        area.ymax - cy,
    )
    angle = math.radians(angle_deg)

    for n in range(points):
        radius = step * math.sqrt(n)
        if radius > max_radius:
            break
        theta = n * angle
        x = cx + radius * math.cos(theta)
        y = cy + radius * math.sin(theta)
        yield area.clamp(x, y)


def hilbert_curve(area: WorkArea, order: int = 6) -> Iterator[Point]:
    """Space-filling Hilbert curve over the largest inscribed square in the work area."""
    if order < 1:
        raise ValueError("Order must be >= 1")

    grid_size    = 2 ** order
    total_points = grid_size * grid_size

    usable_width  = area.xmax - area.xmin
    usable_height = area.ymax - area.ymin
    size     = min(usable_width, usable_height)
    origin_x = (area.xmin + area.xmax - size) / 2
    origin_y = (area.ymin + area.ymax - size) / 2

    def d2xy(n: int, d: int) -> Tuple[int, int]:
        x = y = 0
        t = d
        s = 1
        while s < n:
            rx = 1 & (t // 2)
            ry = 1 & (t ^ rx)
            if ry == 0:
                if rx == 1:
                    x, y = s - 1 - x, s - 1 - y
                x, y = y, x
            x += s * rx
            y += s * ry
            t //= 4
            s *= 2
        return x, y

    denom = max(grid_size - 1, 1)
    for d in range(total_points):
        gx, gy = d2xy(grid_size, d)
        x = origin_x + (gx / denom) * size
        y = origin_y + (gy / denom) * size
        yield area.clamp(x, y)


def resolve_pattern(pattern: PatternInput) -> PatternGenerator:
    """Return a callable pattern from a name or callable input."""
    if pattern is None:
        return DEFAULT_PATTERN
    if isinstance(pattern, str):
        try:
            return PATTERN_REGISTRY[pattern]
        except KeyError as exc:
            raise KeyError(
                f"Unknown pattern '{pattern}'. Available: {', '.join(available_patterns())}"
            ) from exc
    return pattern


def available_patterns() -> Tuple[str, ...]:
    """List registered pattern names."""
    return tuple(PATTERN_REGISTRY.keys())


DEFAULT_PATTERN = center_out_refined_spiral
PATTERN_REGISTRY: Dict[str, PatternGenerator] = {
    "center_out_refined_spiral": center_out_refined_spiral,
    "serpentine_100": lambda area: serpentine_grid(area, spacing=100.0),
    "progressive_raster": lambda area: progressive_raster(
        area, initial_spacing=300.0, passes=4, spacing_decay=0.6
    ),
    "concentric_squares": concentric_square_rings,
    "radial_spokes": radial_spokes,
    "phyllotaxis": phyllotaxis_fill,
    "hilbert": hilbert_curve,
}

def run_rover(x: float, y: float, config: dict) -> None:
    """
    Move the plotter to (x, y) using settings from config.

    Parameters
    ----------
    x, y   : target position in mm
    config : rover_config.json dict (loaded by the caller)
    """
    port      = config["serial_port"]
    baudrate  = int(config.get("baudrate", 115200))
    feed_rate = float(config["feed_rate"])
    home_after_move = bool(config.get("home_after_move", True))

    wa_cfg = config.get("work_area", {})
    area   = WorkArea(
        width  = float(wa_cfg.get("width",  1250.0)),
        height = float(wa_cfg.get("height", 1250.0)),
        margin = float(wa_cfg.get("margin",   10.0)),
    )

    clamped_x, clamped_y = area.clamp(x, y)
    if (clamped_x, clamped_y) != (x, y):
        print(
            f"[rover] WARNING: target ({x:.3f}, {y:.3f}) outside work area "
            f"– clamped to ({clamped_x:.3f}, {clamped_y:.3f})"
        )
        x, y = clamped_x, clamped_y

    print(f"[rover] moving to X={x:.3f}  Y={y:.3f}  feed={feed_rate}  port={port}")
    t_start = time.time()

    with XYPlotter(port, baudrate=baudrate) as plotter:
        #plotter.home()
        plotter.move(x, y, feed_rate=feed_rate)
        if home_after_move:
            plotter.move_to_origin()

    print(f"[rover] move complete in {round(time.time() - t_start, 3)} s")