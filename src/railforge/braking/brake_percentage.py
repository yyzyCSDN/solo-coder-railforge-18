from __future__ import annotations

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True)
class Vehicle:
    id: str
    gross_tonnes: float
    braked_tonnes: float
    operative: bool = True


@dataclass(frozen=True)
class BrakeAssessment:
    total_vehicles: int
    available_vehicles: int
    unavailable_vehicle_ids: tuple[str, ...]
    gross_tonnes: float
    available_braked_tonnes: float
    raw_percentage: float
    gradient_penalty: float
    brake_percentage: float


def _number(value: float, *, positive: bool = False) -> float:
    if not isfinite(value):
        raise ValueError("numeric value must be finite")
    if positive and value <= 0:
        raise ValueError("weight must be positive")
    if not positive and value < 0:
        raise ValueError("weight cannot be negative")
    return value


def assess_brakes(rows: list[Vehicle], gradient_permille: float) -> BrakeAssessment:
    if not rows:
        raise ValueError("no vehicles")
    _number(gradient_permille)

    gross_tonnes = 0.0
    available_braked_tonnes = 0.0
    available_vehicles = 0
    unavailable_vehicle_ids: list[str] = []

    for vehicle in rows:
        gross = _number(vehicle.gross_tonnes, positive=True)
        braked = _number(vehicle.braked_tonnes)
        gross_tonnes += gross
        if vehicle.operative:
            available_vehicles += 1
            available_braked_tonnes += min(gross, braked)
        else:
            unavailable_vehicle_ids.append(vehicle.id)

    raw_percentage = available_braked_tonnes / gross_tonnes * 100
    gradient_penalty = max(0.0, gradient_permille) * 0.35
    percentage = max(0.0, raw_percentage - gradient_penalty)

    return BrakeAssessment(
        total_vehicles=len(rows),
        available_vehicles=available_vehicles,
        unavailable_vehicle_ids=tuple(unavailable_vehicle_ids),
        gross_tonnes=gross_tonnes,
        available_braked_tonnes=available_braked_tonnes,
        raw_percentage=raw_percentage,
        gradient_penalty=gradient_penalty,
        brake_percentage=percentage,
    )


def brake_percentage(rows: list[Vehicle], gradient_permille: float) -> float:
    return assess_brakes(rows, gradient_permille).brake_percentage


def meets(rows: list[Vehicle], gradient_permille: float, required_percent: float) -> bool:
    return brake_percentage(rows, gradient_permille) >= required_percent
