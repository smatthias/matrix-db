"""In-memory catalogue; durable storage is a separate milestone."""
from datetime import datetime
from typing import Literal
from pydantic import Field, field_validator, model_validator
from .contracts import Contract, Coverage, Dataset, Observation

class TimeCoverage(Contract):
    start: datetime
    end: datetime
    _explicit = field_validator('start', 'end', mode='before')(Observation.explicit_timestamp.__func__)
    _utc = field_validator('start', 'end')(Observation.utc_time.__func__)

    @model_validator(mode='after')
    def ordered(self):
        if self.start > self.end:
            raise ValueError('coverage start must not follow end')
        return self

class CatalogueEntry(Contract):
    dataset: Dataset
    source_id: str = Field(min_length=1)
    source_url: str | None = None
    attribution: str | None = None
    contact: str | None = None
    update_interval_seconds: float | None = Field(default=None, gt=0)
    declared_time: TimeCoverage | None = None
    ingested_time: TimeCoverage | None = None
    vertical_coverage: tuple[float, float] | None = None
    vertical_reference: str | None = None
    vertical_positive: Literal['up', 'down'] | None = None
    vertical_unit: Literal['m'] | None = None
    status: Literal['active', 'stale', 'retired'] = 'active'
    last_successful_ingestion: datetime | None = None
    _explicit = field_validator('last_successful_ingestion', mode='before')(Observation.explicit_timestamp.__func__)
    _utc = field_validator('last_successful_ingestion')(Observation.utc_time.__func__)

    @model_validator(mode='after')
    def vertical(self):
        values = (self.vertical_coverage, self.vertical_reference, self.vertical_positive, self.vertical_unit)
        if any(v is not None for v in values) and not all(v is not None for v in values):
            raise ValueError('vertical coverage requires bounds, reference, direction and unit')
        if self.vertical_coverage and self.vertical_coverage[0] > self.vertical_coverage[1]:
            raise ValueError('vertical bounds must be ordered')
        return self


def intersects(a: Coverage, b: Coverage) -> bool:
    def intervals(box):
        return [(box.west, box.east)] if box.west <= box.east else [(box.west, 180), (-180, box.east)]
    return (a.south <= b.north and b.south <= a.north and
            any(x <= v and u <= y for x,y in intervals(a) for u,v in intervals(b)))

class Catalogue:
    def __init__(self, entries=()):
        self._entries = {}
        for entry in entries:
            self.register(entry)

    def register(self, entry: CatalogueEntry) -> CatalogueEntry:
        entry = CatalogueEntry.model_validate_json(entry.model_dump_json())
        if entry.dataset.id in self._entries:
            raise ValueError('dataset ID already registered; use update')
        self._check_source(entry)
        self._entries[entry.dataset.id] = entry
        return entry.model_copy(deep=True)

    def _check_source(self, entry):
        for existing in self._entries.values():
            if (existing.dataset.id != entry.dataset.id and
                (existing.dataset.provider, existing.source_id) == (entry.dataset.provider, entry.source_id)):
                raise ValueError('source ID already registered for this provider')

    def get(self, dataset_id: str) -> CatalogueEntry:
        return self._entries[dataset_id].model_copy(deep=True)

    def update(self, dataset_id: str, replacement: CatalogueEntry) -> CatalogueEntry:
        if dataset_id not in self._entries:
            raise KeyError(dataset_id)
        if replacement.dataset.id != dataset_id:
            raise ValueError('updates must preserve dataset ID')
        replacement = CatalogueEntry.model_validate_json(replacement.model_dump_json())
        self._check_source(replacement)
        self._entries[dataset_id] = replacement
        return replacement.model_copy(deep=True)

    def search(self, *, variable=None, provider=None, origin=None, status=None,
               region: Coverage | None = None, time: TimeCoverage | None = None,
               availability: Literal['declared', 'ingested'] = 'declared') -> list[CatalogueEntry]:
        if availability not in ('declared', 'ingested'):
            raise ValueError('availability must be declared or ingested')
        result = []
        for entry in self._entries.values():
            dataset = entry.dataset
            if variable is not None and variable not in {v.id for v in dataset.variables}: continue
            if provider is not None and dataset.provider != provider: continue
            if origin is not None and dataset.origin != origin: continue
            if status is not None and entry.status != status: continue
            if region is not None and not intersects(dataset.coverage, region): continue
            coverage = entry.declared_time if availability == 'declared' else entry.ingested_time
            if time is not None and (coverage is None or coverage.start > time.end or time.start > coverage.end): continue
            result.append(entry.model_copy(deep=True))
        return sorted(result, key=lambda entry: entry.dataset.id)
