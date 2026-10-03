from __future__ import annotations

from dataclasses import dataclass, is_dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal
from threading import RLock
from typing import Any, Callable, Mapping, TypeVar

from railforge.audit.trail import Trail
from railforge.braking.brake_percentage import assess_brakes
from railforge.billing.demurrage import charge
from railforge.cargo.segregation import violations
from railforge.consist.integrity import validate
from railforge.customs.holds import active
from railforge.energy.regen import net_energy
from railforge.eta.event_fusion import fused_delay
from railforge.events.cursor import CursorConsumer, FeedEvent
from railforge.events.stream import EventStream
from railforge.intermodal.connections import feasible
from railforge.inventory.wagon_pool import allocate
from railforge.locomotive.tractive_effort import available_effort
from railforge.maintenance.release import releasable
from railforge.possession.windows import conflicts as possession_conflicts
from railforge.routing.clearance import route_ok
from railforge.signaling.block_occupancy import reserve
from railforge.storage.ops_store import VersionedStore
from railforge.timetable.meets import conflicts as meet_conflicts
from railforge.wagon.axle_load import route_ok as axle_route_ok
from railforge.yard.hump import classify
from railforge.yard.switching import lock_route
from railforge.crew.duty import legal as duty_legal

T = TypeVar("T")


def _safe(value: Any) -> Any:
    if is_dataclass(value):
        return {k: _safe(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): _safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_safe(v) for v in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


@dataclass(frozen=True)
class OperationReceipt:
    operation_id: str
    topic: str
    subject: str
    result: Any
    event: Any
    audit_index: int
    version: int


class RailForgeWorkflowControl:
    """Atomic domain -> event -> audit workflow used by every feature surface."""

    def __init__(self, stream: EventStream | None = None, trail: Trail | None = None,
                 store: VersionedStore | None = None):
        self.stream = stream or EventStream()
        self.trail = trail or Trail()
        self.store = store or VersionedStore()
        self._receipts: dict[str, OperationReceipt] = {}
        self._consumers: dict[str, CursorConsumer] = {}
        self._lock = RLock()
        self._committing = False

    @property
    def receipts(self) -> Mapping[str, OperationReceipt]:
        return dict(self._receipts)

    def _commit(self, operation_id: str, topic: str, subject: str, at: datetime,
                action: Callable[[], T], evidence: Mapping[str, Any] | None = None) -> OperationReceipt:
        with self._lock:
            previous = self._receipts.get(operation_id)
            if previous is not None:
                if previous.topic != topic or previous.subject != subject:
                    raise ValueError("operation id reused for a different workflow")
                return previous
            if self._committing:
                raise RuntimeError("nested workflow commits are not atomic")

            store_snapshot = self.store.snapshot()
            trail_snapshot = self.trail.snapshot()
            previous_stream_head = self.stream.head
            evidence_payload = dict(evidence or {})
            reserved_payload_keys = {
                "operation_id", "status", "control_path", "version", "result",
                "audit_index", "audit_digest",
            }
            if reserved_payload_keys.intersection(evidence_payload):
                raise ValueError("evidence contains reserved workflow fields")

            self._committing = True
            try:
                result = action()
                safe_result = _safe(result)
                version = self.store.write(subject, safe_result)
                audit = self.trail.append("railforge-control", topic, subject, at)
                payload = dict(evidence_payload)
                payload.update({
                    "operation_id": operation_id,
                    "status": "committed",
                    "control_path": "atomic",
                    "version": version,
                    "result": safe_result,
                    "audit_index": audit.index,
                    "audit_digest": audit.digest,
                })
                event = self.stream.publish(operation_id, at, topic, subject, payload)
                if event.sequence <= previous_stream_head:
                    raise RuntimeError("operation id already published")
                receipt = OperationReceipt(operation_id, topic, subject, result, event, audit.index, version)
                self._receipts[operation_id] = receipt
                return receipt
            except Exception:
                self.store.restore(store_snapshot)
                self.trail.restore(trail_snapshot)
                raise
            finally:
                self._committing = False

    def replay(self) -> dict[str, dict[str, Any]]:
        state: dict[str, dict[str, Any]] = {}
        with self._lock:
            events = self.stream.snapshot()
            audit_rows = self.trail.snapshot()
            control_events = 0
            audited_indices: set[int] = set()
            for event in events:
                audit_index = event.payload.get("audit_index")
                if audit_index is None:
                    continue
                if not isinstance(audit_index, int) or audit_index < 1 or audit_index > len(audit_rows):
                    raise ValueError("event is missing audit linkage")
                if audit_index in audited_indices:
                    raise ValueError("audit and event stream disagree")
                audited_indices.add(audit_index)
                audit = audit_rows[audit_index - 1]
                if audit.index != audit_index or audit.digest != event.payload.get("audit_digest"):
                    raise ValueError("audit and event stream disagree")
                control_events += 1
                if event.payload.get("status") != "committed":
                    raise ValueError("event is not committed")
                version = event.payload.get("version")
                if not isinstance(version, int) or version < 1:
                    raise ValueError("event is missing state version")
                subject_state = state.setdefault(event.subject, {})
                if subject_state.get("version", 0) != version - 1:
                    raise ValueError("state versions do not replay in order")
                subject_state["version"] = version
                subject_state["result"] = event.payload.get("result")
            if control_events != len(audit_rows):
                raise ValueError("audit and event stream disagree")
            for subject, stored in self.store.snapshot().items():
                replayed = state.get(subject)
                if replayed is None or replayed["version"] != stored.version or replayed["result"] != stored.value:
                    raise ValueError("replayed state does not match the store")
            return state

    def replay_consistent(self) -> bool:
        if not self.stream.verify_chain() or not self.trail.verify():
            return False
        try:
            self.replay()
        except (IndexError, ValueError):
            return False
        return True

    def run(self, operation_id: str, topic: str, subject: str, at: datetime,
            action: Callable[[], T], evidence: Mapping[str, Any] | None = None) -> OperationReceipt:
        return self._commit(operation_id, topic, subject, at, action, evidence)

    def reserve_block(self, operation_id, candidate, existing, clearance, at):
        return self._commit(operation_id, "dispatch.block.reserved", candidate.train, at,
                            lambda: reserve(candidate, existing, clearance))

    def validate_consist(self, operation_id, rows, subject, at):
        return self._commit(operation_id, "consist.validated", subject, at, lambda: validate(rows))

    def assess_brakes(self, operation_id, rows, gradient, subject, at):
        return self._commit(operation_id, "braking.assessed", subject, at,
                            lambda: assess_brakes(rows, gradient).brake_percentage)

    def plan_traction(self, operation_id, rows, adhesion_factor, subject, at):
        return self._commit(operation_id, "traction.planned", subject, at,
                            lambda: available_effort(rows, adhesion_factor))

    def assign_duty(self, operation_id, rows, max_duty, max_drive, max_span, subject, at):
        def action():
            result = duty_legal(rows, max_duty, max_drive, max_span)
            if not result:
                raise ValueError("duty is not legal")
            return result
        return self._commit(operation_id, "crew.duty.assigned", subject, at, action)

    def book_meet(self, operation_id, rows, subject, at):
        return self._commit(operation_id, "timetable.meet.booked", subject, at,
                            lambda: meet_conflicts(rows))

    def classify_yard(self, operation_id, cuts, tracks, occupied, subject, at):
        return self._commit(operation_id, "yard.classified", subject, at,
                            lambda: classify(cuts, tracks, occupied))

    def check_axles(self, operation_id, wagon, max_axle, max_gross, subject, at):
        return self._commit(operation_id, "wagon.axle.checked", subject, at,
                            lambda: axle_route_ok(wagon, max_axle, max_gross))

    def book_intermodal(self, operation_id, inbound, outbound, unload, load, cutoff, subject, at):
        return self._commit(operation_id, "intermodal.booked", subject, at,
                            lambda: feasible(inbound, outbound, unload, load, cutoff))

    def protect_possession(self, operation_id, candidate, rows, subject, at):
        return self._commit(operation_id, "possession.protected", subject, at,
                            lambda: possession_conflicts(candidate, rows))

    def release_maintenance(self, operation_id, evidence, policy, check_at, subject, at):
        def action():
            result = releasable(evidence, policy, check_at)
            if not result:
                raise ValueError("maintenance evidence incomplete")
            return result
        return self._commit(operation_id, "maintenance.released", subject, at, action)

    def consume_event(self, operation_id, event: FeedEvent, subject, at):
        consumer = self._consumers.setdefault(event.partition, CursorConsumer())
        return self._commit(operation_id, "events.cursor.consumed", subject, at,
                            lambda: consumer.accept(event))

    def charge_demurrage(self, operation_id, arrival, release, tariff, subject, at):
        return self._commit(operation_id, "billing.demurrage.charged", subject, at,
                            lambda: charge(arrival, release, tariff))

    def check_customs(self, operation_id, rows, scope, check_at, subject, at):
        return self._commit(operation_id, "customs.hold.checked", subject, at,
                            lambda: active(rows, scope, check_at))

    def check_segregation(self, operation_id, rows, rules, subject, at):
        return self._commit(operation_id, "cargo.segregation.checked", subject, at,
                            lambda: violations(rows, rules))

    def calculate_energy(self, operation_id, rows, subject, at):
        return self._commit(operation_id, "energy.calculated", subject, at,
                            lambda: net_energy(rows))

    def lock_switch_route(self, operation_id, route, switches, owner, subject, at):
        return self._commit(operation_id, "yard.route.locked", subject, at,
                            lambda: lock_route(route, switches, owner))

    def check_clearance(self, operation_id, profile, segments, subject, at):
        return self._commit(operation_id, "routing.clearance.checked", subject, at,
                            lambda: route_ok(profile, segments))

    def allocate_wagons(self, operation_id, rows, wagon_type, location, on, count, required_days, forbidden, subject, at):
        return self._commit(operation_id, "inventory.wagons.allocated", subject, at,
                            lambda: allocate(rows, wagon_type, location, on, count, required_days, forbidden))

    def fuse_eta(self, operation_id, rows, max_lateness, subject, at):
        return self._commit(operation_id, "eta.fused", subject, at,
                            lambda: fused_delay(rows, max_lateness))
