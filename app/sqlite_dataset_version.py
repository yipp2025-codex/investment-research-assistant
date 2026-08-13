"""Query-only M9 adapter for an explicit persisted v12 dataset version.

The adapter is intentionally a consumer boundary.  It accepts one immutable
``dataset_version_id`` and reconstructs the M9 read models from the v12
dataset-version tables through the existing strict repository replay path.  It
does not select a latest row, infer a version from symbol/date, contact a
provider, or write any database state.
"""

from __future__ import annotations

from datetime import datetime, time, timezone
from pathlib import Path

from app.data_contracts.dual_source import SourceRole
from app.research_dataset import (
    DatasetAsOf,
    DatasetArtifactRef,
    DatasetDiscrepancy,
    DatasetPrice,
    DatasetProvenance,
    DatasetSymbol,
    DatasetValidationObservation,
    PriceHistoryReadModel,
    ResearchDataset,
    ResearchDatasetRequest,
    ResearchDatasetSnapshot,
    ValidationReadModel,
    ValuationReadModel,
)
from app.storage.dataset_versions import (
    DatasetPersistenceIntegrityError,
    DatasetVersionNotFoundError,
    DatasetVersionRepository,
)


DUAL_SOURCE_POLICY = "twse_dual_source_v1"
_UTC = timezone.utc


class DatasetVersionReadError(RuntimeError):
    """The explicit v12 dataset version cannot be exposed as an M9 snapshot."""


class SQLiteDatasetVersionResearchDataset:
    """Read one explicit v12 dataset version as an M9 ``ResearchDataset``."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self._repository = DatasetVersionRepository(self.database_path)

    def read(
        self,
        request: ResearchDatasetRequest,
        /,
    ) -> ResearchDatasetSnapshot:
        if not isinstance(request, ResearchDatasetRequest):
            raise TypeError("request must be a ResearchDatasetRequest")
        dataset_version_id = request.dataset_version_id
        if dataset_version_id is None:
            raise DatasetVersionReadError(
                "v12 M9 reads require an explicit dataset_version_id; implicit latest lookup is forbidden"
            )
        if request.pipeline_run_id or request.historical_run_id or request.validation_run_id:
            raise DatasetVersionReadError(
                "v12 M9 reads accept dataset_version_id only; legacy run selectors cannot be mixed"
            )
        try:
            version = self._repository.replay(dataset_version_id)
        except DatasetVersionNotFoundError as error:
            raise DatasetVersionReadError(
                f"dataset version {dataset_version_id} is not persisted"
            ) from error
        except DatasetPersistenceIntegrityError as error:
            raise DatasetVersionReadError(
                "persisted v12 dataset version failed closed during M9 reconstruction"
            ) from error

        identity = version.identity
        if identity.symbol != request.symbol or identity.as_of_date != request.as_of_date:
            raise DatasetVersionReadError(
                "explicit dataset_version_id does not match the requested symbol/as_of_date"
            )

        selected = sorted(
            (item for item in version.observations if item.selected),
            key=lambda item: item.trade_date,
        )
        prices = tuple(
            DatasetPrice(
                symbol=item.symbol,
                trade_date=item.trade_date,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
                source=item.provider,
                source_role=item.source_role.value,
            )
            for item in selected
        )
        requested = request.history_observations
        returned = prices if requested is None else prices[-requested:]
        current = returned[-1] if returned and returned[-1].trade_date == request.as_of_date else None
        if not returned:
            history_status = "missing_source"
        elif current is not None:
            history_status = "available"
        else:
            history_status = "market_date_mismatch"
        history = PriceHistoryReadModel(
            symbol=request.symbol,
            as_of_date=request.as_of_date,
            status=history_status,
            observations=returned,
            current=current,
            total_observations_as_of=len(prices),
            requested_observations=requested,
            is_truncated=len(returned) < len(prices),
        )

        provenance = self._provenance(version)
        validation = self._validation(
            version=version,
            prices=prices,
            request=request,
        )
        as_of = request.as_of_date
        return ResearchDatasetSnapshot(
            symbol=DatasetSymbol(
                symbol=identity.symbol,
                name=None,
                market="TWSE",
                currency="TWD",
            ),
            as_of=DatasetAsOf(
                as_of_date=as_of,
                history_observations=requested,
                total_history_observations=len(prices),
                returned_history_observations=len(returned),
                history_is_truncated=len(returned) < len(prices),
                source_policy=identity.source_policy,
            ),
            price_history=history,
            valuation=ValuationReadModel(
                symbol=identity.symbol,
                as_of_date=as_of,
                status="missing_source",
                metrics=(),
            ),
            validation=validation,
            provenance=provenance,
        )

    @staticmethod
    def _provenance(version: object) -> DatasetProvenance:
        identity = version.identity
        observations = version.observations
        canonical_sources = tuple(
            sorted(
                {
                    item.provider
                    for item in observations
                    if item.selected and item.source_role is SourceRole.CANONICAL
                }
            )
        )
        supplemental_sources = tuple(
            sorted(
                {
                    item.provider
                    for item in observations
                    if item.selected and item.source_role is SourceRole.SUPPLEMENTAL
                }
            )
        )
        validation_sources = tuple(
            sorted(
                {
                    item.provider
                    for item in observations
                    if item.source_role is SourceRole.VALIDATION
                }
            )
        )
        artifacts = tuple(
            DatasetArtifactRef(
                owner_kind="dataset_version",
                owner_run_id=identity.dataset_version_id,
                provider=item.provider,
                dataset=item.dataset,
                endpoint=item.source_ref,
                contract_version=item.contract_version,
                content_type="application/json",
                payload_sha256=item.payload_sha256,
                payload_size_bytes=item.payload_size_bytes,
                hash_basis=item.hash_basis,
            )
            for item in version.artifacts
        )
        coverage = identity.coverage
        return DatasetProvenance(
            symbol=identity.symbol,
            canonical_sources=canonical_sources,
            validation_sources=validation_sources,
            supplemental_sources=supplemental_sources,
            artifact_refs=artifacts,
            source_policy=identity.source_policy,
            dataset_version_id=identity.dataset_version_id,
            source_status=identity.source_status.value,
            authority_status=identity.authority_status.value,
            reconciliation_status=identity.reconciliation_status.value,
            research_data_quality={
                "canonical_complete": "canonical",
                "provisional_mixed": "provisional",
                "reconciled": "reconciled",
            }[identity.source_status.value],
            canonical_authority=version.provenance_summary.canonical_authority,
            twse_observation_count=coverage.twse_observation_count,
            esun_supplemental_count=coverage.esun_supplemental_count,
            missing_twse_count=coverage.missing_twse_count,
            discrepancy_count=coverage.discrepancy_count,
            provenance_map_sha256=identity.provenance_map_sha256,
            parent_dataset_version_id=identity.parent_dataset_version_id,
        )

    @staticmethod
    def _validation(*, version: object, prices: tuple[DatasetPrice, ...], request: ResearchDatasetRequest) -> ValidationReadModel:
        validation_rows = tuple(
            item
            for item in version.observations
            if item.source_role is SourceRole.VALIDATION
            and item.trade_date == request.as_of_date
        )
        if not validation_rows:
            return ValidationReadModel(symbol=request.symbol)
        canonical = next(
            (item for item in prices if item.trade_date == request.as_of_date and item.source_role == "canonical"),
            None,
        )
        if canonical is None:
            return ValidationReadModel(symbol=request.symbol)
        provider_rows = {item.provider: item for item in validation_rows}
        right = next(iter(sorted(provider_rows)))
        right_row = provider_rows[right]
        left_observation = DatasetValidationObservation(
            symbol=request.symbol,
            provider=canonical.source,
            market_date=request.as_of_date,
            open=canonical.open,
            high=canonical.high,
            low=canonical.low,
            close=canonical.close,
            volume=canonical.volume,
            source_endpoints=("dataset://canonical",),
            fetched_at=datetime.combine(request.as_of_date, time.min, tzinfo=_UTC),
        )
        right_observation = DatasetValidationObservation(
            symbol=request.symbol,
            provider=right_row.provider,
            market_date=request.as_of_date,
            open=right_row.open,
            high=right_row.high,
            low=right_row.low,
            close=right_row.close,
            volume=right_row.volume,
            source_endpoints=("dataset://validation",),
            fetched_at=datetime.combine(request.as_of_date, time.min, tzinfo=_UTC),
        )
        discrepancies = tuple(
            DatasetDiscrepancy(field, left, right, "persisted validation observation differs")
            for field, left, right in (
                ("open", left_observation.open, right_observation.open),
                ("high", left_observation.high, right_observation.high),
                ("low", left_observation.low, right_observation.low),
                ("close", left_observation.close, right_observation.close),
                ("volume", left_observation.volume, right_observation.volume),
            )
            if left != right
        )
        status = "source_discrepancy" if discrepancies else "available"
        return ValidationReadModel(
            symbol=request.symbol,
            status=status,
            run_id=version.identity.dataset_version_id,
            target_date=request.as_of_date,
            left_provider=canonical.source,
            right_provider=right_observation.provider,
            outcome="discrepancy" if discrepancies else "match",
            created_at=datetime.combine(request.as_of_date, time.min, tzinfo=_UTC),
            observations=(left_observation, right_observation),
            discrepancies=discrepancies,
        )


# Friendly aliases keep the read boundary discoverable without duplicating it.
SQLiteV12ResearchDataset = SQLiteDatasetVersionResearchDataset
SQLiteResearchDatasetV12 = SQLiteDatasetVersionResearchDataset


__all__ = [
    "DUAL_SOURCE_POLICY",
    "DatasetVersionReadError",
    "SQLiteDatasetVersionResearchDataset",
    "SQLiteResearchDatasetV12",
    "SQLiteV12ResearchDataset",
]
