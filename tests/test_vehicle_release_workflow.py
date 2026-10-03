import unittest
from datetime import datetime, timedelta, timezone

from railforge.api import RailForgeService
from railforge.braking.brake_percentage import Vehicle, assess_brakes, brake_percentage
from railforge.maintenance.release import Evidence, ReleasePolicy

UTC = timezone.utc


class VehicleReleaseWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.service = RailForgeService()
        self.at = datetime(2026, 1, 1, tzinfo=UTC)
        self.evidence = [
            Evidence("brake-test", self.at - timedelta(minutes=5), timedelta(days=1), True),
        ]
        self.policy = ReleasePolicy(frozenset({"brake-test"}))
        self.vehicles = [
            Vehicle("W1", 100, 90, True),
            Vehicle("W2", 100, 100, False),
        ]

    def release(self, operation="release-1", vehicles=None, evidence=None, gradient=10):
        return self.service.features.release_vehicle(
            operation,
            evidence or self.evidence,
            self.policy,
            self.at,
            self.vehicles if vehicles is None else vehicles,
            gradient,
            self.at,
        )

    def test_unavailable_vehicle_braked_tonnage_is_excluded(self):
        self.assertEqual(brake_percentage(self.vehicles, 10), 41.5)
        assessment = assess_brakes(self.vehicles, 10)
        self.assertEqual(assessment.available_vehicles, 1)
        self.assertEqual(assessment.total_vehicles, 2)
        self.assertEqual(assessment.unavailable_vehicle_ids, ("W2",))
        self.assertEqual(assessment.raw_percentage, 45)
        self.assertEqual(assessment.gradient_penalty, 3.5)

    def test_release_commits_one_integrated_event(self):
        receipt = self.release()

        self.assertTrue(receipt.result["released"])
        self.assertEqual(receipt.result["brake_percentage"], 41.5)
        self.assertEqual(receipt.result["unavailable_vehicle_ids"], ["W2"])
        self.assertEqual(self.service.control.stream.head, 1)
        self.assertEqual(receipt.event.topic, "feature.vehicle.release")
        self.assertEqual(receipt.event.payload["status"], "committed")
        self.assertEqual(receipt.event.payload["control_path"], "atomic")
        self.assertEqual(len(self.service.control.trail.snapshot()), 1)
        self.assertTrue(self.service.control.replay_consistent())

        topics = [event.topic for event in self.service.control.stream.snapshot()]
        self.assertEqual(topics, ["feature.vehicle.release"])

    def test_maintenance_rejection_leaves_no_partial_record(self):
        evidence = [Evidence("brake-test", self.at, timedelta(days=1), False)]

        with self.assertRaises(ValueError):
            self.release(evidence=evidence)

        self.assertEqual(self.service.control.stream.head, 0)
        self.assertEqual(self.service.control.trail.snapshot(), ())
        self.assertEqual(self.service.control.store.snapshot(), {})
        self.assertTrue(self.service.control.replay_consistent())

    def test_invalid_brake_input_leaves_no_partial_record(self):
        with self.assertRaises(ValueError):
            self.release(vehicles=[Vehicle("W1", 0, 0, True)])

        self.assertEqual(self.service.control.stream.head, 0)
        self.assertEqual(self.service.control.trail.snapshot(), ())
        self.assertEqual(self.service.control.store.snapshot(), {})
        self.assertTrue(self.service.control.replay_consistent())


if __name__ == "__main__":
    unittest.main()
