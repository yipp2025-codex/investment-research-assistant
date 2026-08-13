"""DS4 reconciliation persistence on top of the frozen DS3 v12 substrate.

No new migration is introduced.  The immutable relation is represented by a
typed hash-only row in ``research_dataset_artifacts`` appended to the child;
the child identity already stores the parent id, and replay reconstructs and
verifies the complete relation deterministically.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.data_contracts.dataset_persistence import (
    DatasetObservation,
    MixedDatasetVersion,
)
from app.data_contracts.dual_source import SourceRole
from app.data_contracts.reconciliation import (
    RECONCILIATION_RELATION_DATASET,
    ReconciliationContractError,
    ReconciliationInput,
    ReconciliationIntegrityError,
    ReconciliationResult,
    reconcile_dataset_version,
)

from .dataset_versions import (
    DatasetPersistenceIntegrityError,
    DatasetVersionNotFoundError,
    DatasetVersionRepository,
)


class ReconciliationPersistenceError(DatasetPersistenceIntegrityError):
    """The persisted DS4 lineage cannot be verified or safely replayed."""


@dataclass(frozen=True, slots=True)
class ReconciliationPersistenceResult:
    reconciliation: ReconciliationResult
    written: bool
    created_at: str

    @property
    def child_dataset_version_id(self) -> str:
        return self.reconciliation.new_dataset_version.identity.dataset_version_id

    @property
    def reconciliation_evidence_sha256(self) -> str:
        return self.reconciliation.reconciliation_evidence_sha256


class DatasetReconciliationRepository:
    """Persist DS4 children and replay their parent/relation lineage strictly."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        dataset_repository: DatasetVersionRepository | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self.dataset_repository = dataset_repository or DatasetVersionRepository(
            self.database_path
        )

    def reconcile_and_save(
        self, input_value: ReconciliationInput
    ) -> ReconciliationPersistenceResult:
        if not isinstance(input_value, ReconciliationInput):
            raise ReconciliationPersistenceError("input must be a ReconciliationInput")
        try:
            stored_parent = self.dataset_repository.replay(
                input_value.parent_dataset_version_id
            )
            self._assert_same_parent(input_value.parent, stored_parent)
            self._assert_parent_chain(stored_parent)
            result = reconcile_dataset_version(input_value)
            saved = self.dataset_repository.save(result.new_dataset_version)
            verified = self.replay(result.new_dataset_version.identity.dataset_version_id)
            if verified.reconciliation.as_dict() != result.as_dict():
                raise ReconciliationPersistenceError(
                    "persisted reconciliation replay differs from the pure result"
                )
            return ReconciliationPersistenceResult(
                reconciliation=verified.reconciliation,
                written=saved.written,
                created_at=saved.created_at,
            )
        except ReconciliationPersistenceError:
            raise
        except (ReconciliationContractError, ReconciliationIntegrityError) as exc:
            raise ReconciliationPersistenceError(str(exc)) from exc
        except (DatasetPersistenceIntegrityError, DatasetVersionNotFoundError) as exc:
            raise ReconciliationPersistenceError(str(exc)) from exc

    def replay(self, child_dataset_version_id: str) -> ReconciliationPersistenceResult:
        """Replay one DS4 child and verify its current relation artifact."""

        try:
            child = self.dataset_repository.replay(child_dataset_version_id)
            parent_id = child.identity.parent_dataset_version_id
            if parent_id is None:
                raise ReconciliationPersistenceError(
                    "DS4 child must have a parent_dataset_version_id"
                )
            parent = self.dataset_repository.replay(parent_id)
            self._assert_parent_chain(parent, child_dataset_version_id)
            relation_artifact = self._current_relation_artifact(child)
            source_artifact = self._source_artifact_before_relation(child, relation_artifact.ordinal)
            compared_dates = tuple(
                sorted(
                    {
                        item.trade_date
                        for item in parent.observations
                        if item.selected and item.source_role is SourceRole.SUPPLEMENTAL
                    }
                    - {
                        item.trade_date
                        for item in child.observations
                        if item.selected and item.source_role is SourceRole.SUPPLEMENTAL
                    }
                )
            )
            if not compared_dates:
                raise ReconciliationPersistenceError(
                    "DS4 child relation has no transitioned supplemental dates"
                )
            twse_rows = self._child_twse_rows(child, compared_dates)
            source_runs = {item.source_run_id for item in twse_rows}
            if len(source_runs) != 1:
                raise ReconciliationPersistenceError(
                    "DS4 child relation has inconsistent TWSE source runs"
                )
            input_value = ReconciliationInput(
                parent=parent,
                parent_dataset_version_id=parent.identity.dataset_version_id,
                parent_canonical_sha256=parent.canonical_sha256,
                twse_observations=twse_rows,
                twse_source_run_id=next(iter(source_runs)),
                twse_artifact=source_artifact,
                target_dates=compared_dates,
            )
            expected = reconcile_dataset_version(input_value)
            if expected.new_dataset_version.canonical_json() != child.canonical_json():
                raise ReconciliationPersistenceError(
                    "DS4 child canonical replay differs from its stored lineage"
                )
            if relation_artifact.payload_sha256 != expected.reconciliation_evidence_sha256:
                raise ReconciliationPersistenceError(
                    "DS4 relation evidence hash does not match deterministic replay"
                )
            if relation_artifact.payload_size_bytes != len(
                expected.canonical_evidence_json().encode("utf-8")
            ):
                raise ReconciliationPersistenceError(
                    "DS4 relation evidence size does not match deterministic replay"
                )
            return ReconciliationPersistenceResult(
                reconciliation=expected,
                written=False,
                created_at=self._created_at(child_dataset_version_id),
            )
        except ReconciliationPersistenceError:
            raise
        except (ReconciliationContractError, ReconciliationIntegrityError) as exc:
            raise ReconciliationPersistenceError(str(exc)) from exc
        except (DatasetPersistenceIntegrityError, DatasetVersionNotFoundError) as exc:
            raise ReconciliationPersistenceError(str(exc)) from exc

    def _created_at(self, dataset_version_id: str) -> str:
        with self.dataset_repository._read_connection() as connection:  # noqa: SLF001
            row = connection.execute(
                "SELECT created_at FROM research_dataset_versions "
                "WHERE dataset_version_id = ?",
                (dataset_version_id,),
            ).fetchone()
            if row is None:
                raise ReconciliationPersistenceError(
                    "DS4 child disappeared during replay"
                )
            return row["created_at"]

    @staticmethod
    def _assert_same_parent(
        supplied: MixedDatasetVersion, stored: MixedDatasetVersion
    ) -> None:
        if supplied.identity.dataset_version_id != stored.identity.dataset_version_id:
            raise ReconciliationPersistenceError(
                "supplied parent identity differs from persisted parent"
            )
        if supplied.canonical_json() != stored.canonical_json():
            raise ReconciliationPersistenceError(
                "supplied parent canonical bytes differ from persisted parent"
            )

    def _assert_parent_chain(
        self,
        parent: MixedDatasetVersion,
        child_dataset_version_id: str | None = None,
    ) -> None:
        seen: set[str] = set()
        current = parent
        if child_dataset_version_id is not None:
            seen.add(child_dataset_version_id)
        while True:
            current_id = current.identity.dataset_version_id
            if current_id in seen:
                raise ReconciliationPersistenceError(
                    "dataset lineage contains a cycle"
                )
            seen.add(current_id)
            parent_id = current.identity.parent_dataset_version_id
            if parent_id is None:
                return
            try:
                next_parent = self.dataset_repository.replay(parent_id)
            except DatasetPersistenceIntegrityError as exc:
                raise ReconciliationPersistenceError(
                    "dataset lineage parent cannot be replayed"
                ) from exc
            if (
                next_parent.identity.symbol != current.identity.symbol
                or next_parent.identity.as_of_date != current.identity.as_of_date
                or next_parent.identity.contract_version
                != current.identity.contract_version
                or next_parent.identity.source_policy
                != current.identity.source_policy
            ):
                raise ReconciliationPersistenceError(
                    "dataset lineage parent is incompatible with its child"
                )
            current = next_parent

    @staticmethod
    def _current_relation_artifact(child: MixedDatasetVersion):
        relation_artifacts = [
            item
            for item in child.artifacts
            if item.dataset == RECONCILIATION_RELATION_DATASET
        ]
        if not relation_artifacts:
            raise ReconciliationPersistenceError(
                "DS4 child is missing its reconciliation relation artifact"
            )
        relation_artifact = max(relation_artifacts, key=lambda item: item.ordinal)
        if relation_artifact.ordinal != len(child.artifacts):
            raise ReconciliationPersistenceError(
                "current DS4 relation artifact must be the final child artifact"
            )
        expected_ref = (
            f"reconciliation://{child.identity.parent_dataset_version_id}/"
            f"{child.identity.dataset_version_id}"
        )
        if relation_artifact.source_ref != expected_ref:
            raise ReconciliationPersistenceError(
                "DS4 relation artifact parent/child locator is invalid"
            )
        return relation_artifact

    @staticmethod
    def _source_artifact_before_relation(
        child: MixedDatasetVersion, relation_ordinal: int
    ):
        matches = [item for item in child.artifacts if item.ordinal == relation_ordinal - 1]
        if len(matches) != 1:
            raise ReconciliationPersistenceError(
                "DS4 relation is not preceded by exactly one source artifact"
            )
        artifact = matches[0]
        if artifact.dataset == RECONCILIATION_RELATION_DATASET:
            raise ReconciliationPersistenceError(
                "DS4 relation predecessor cannot be another relation artifact"
            )
        if artifact.provider not in {"twse", "twse-historical"}:
            raise ReconciliationPersistenceError(
                "DS4 relation predecessor must be a TWSE artifact"
            )
        return artifact

    @staticmethod
    def _child_twse_rows(
        child: MixedDatasetVersion, compared_dates: tuple
    ) -> tuple[DatasetObservation, ...]:
        rows: list[DatasetObservation] = []
        for trade_date in compared_dates:
            matches = [
                item
                for item in child.observations
                if item.trade_date == trade_date
                and item.selected
                and item.source_role is SourceRole.CANONICAL
            ]
            if len(matches) != 1:
                raise ReconciliationPersistenceError(
                    "DS4 child must contain exactly one selected TWSE row per transitioned date"
                )
            rows.append(matches[0])
        return tuple(rows)


__all__ = [
    "DatasetReconciliationRepository",
    "ReconciliationPersistenceError",
    "ReconciliationPersistenceResult",
]
