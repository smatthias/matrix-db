"""Version-one contracts. Coordinates are WGS84 longitude/latitude."""
from datetime import datetime, timezone
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, model_validator, field_validator

Identifier = Annotated[str, Field(min_length=1, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")]
Number = StrictInt | StrictFloat

class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

class Variable(Contract):
    id: Identifier
    quantity: Annotated[str, Field(min_length=1)]
    unit: Annotated[str, Field(min_length=1)]
    data_type: Literal["float", "integer"] = "float"
    minimum: float | None = None
    maximum: float | None = None
    nullable: bool = True

    @model_validator(mode="after")
    def ordered_range(self):
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError("minimum must not exceed maximum")
        return self

class Coverage(Contract):
    west: float = Field(ge=-180, le=180)
    south: float = Field(ge=-90, le=90)
    east: float = Field(ge=-180, le=180)
    north: float = Field(ge=-90, le=90)

    @model_validator(mode="after")
    def latitude_order(self):
        if self.south > self.north:
            raise ValueError("south must not exceed north")
        return self

    def contains(self, longitude, latitude):
        longitude_inside = (self.west <= longitude <= self.east if self.west <= self.east
                            else longitude >= self.west or longitude <= self.east)
        return longitude_inside and self.south <= latitude <= self.north

class Dataset(Contract):
    schema_version: Literal[1] = 1
    id: Identifier
    name: Annotated[str, Field(min_length=1)]
    provider: Annotated[str, Field(min_length=1)]
    license: Annotated[str, Field(min_length=1)]
    source_reference: Annotated[str, Field(min_length=1)]
    coordinate_system: Literal["EPSG:4326"] = "EPSG:4326"
    coverage: Coverage
    spatial_resolution_metres: float | None = Field(default=None, gt=0)
    temporal_resolution_seconds: float | None = Field(default=None, gt=0)
    origin: Literal["observation", "forecast", "simulation"]
    variables: list[Variable] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_variables(self):
        ids = [v.id for v in self.variables]
        if len(ids) != len(set(ids)):
            raise ValueError("variable IDs must be unique within a dataset")
        return self

class VerticalCoordinate(Contract):
    value: float
    unit: Literal["m"] = "m"
    positive: Literal["up", "down"]
    reference: Annotated[str, Field(min_length=1)]

class Observation(Contract):
    schema_version: Literal[1] = 1
    id: Identifier
    dataset_id: Identifier
    variable_id: Identifier
    longitude: float = Field(ge=-180, le=180)
    latitude: float = Field(ge=-90, le=90)
    valid_time: datetime
    ingested_at: datetime
    forecast_issue_time: datetime | None = None
    vertical: VerticalCoordinate | None = None
    value: Number | None
    unit: Annotated[str, Field(min_length=1)]
    quality: Literal["good", "suspect", "bad", "unknown"] = "unknown"
    source_reference: Annotated[str, Field(min_length=1)]

    @field_validator("valid_time", "ingested_at", "forecast_issue_time", mode="before")
    @classmethod
    def explicit_timestamp(cls, value):
        if value is not None and not isinstance(value, (str, datetime)):
            raise ValueError("timestamp must be an ISO 8601 string or aware datetime")
        return value

    @field_validator("valid_time", "ingested_at", "forecast_issue_time")
    @classmethod
    def utc_time(cls, value):
        if value is not None:
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("timestamp must include a timezone")
            return value.astimezone(timezone.utc)
        return value

    def validate_against(self, dataset: Dataset):
        if self.dataset_id != dataset.id:
            raise ValueError("dataset_id does not match dataset")
        variable = next((v for v in dataset.variables if v.id == self.variable_id), None)
        if variable is None:
            raise ValueError("unknown variable_id")
        if self.unit != variable.unit:
            raise ValueError("unit must match variable definition; convert explicitly before ingestion")
        if not dataset.coverage.contains(self.longitude, self.latitude):
            raise ValueError("location is outside dataset coverage")
        if dataset.origin == "forecast":
            if self.forecast_issue_time is None:
                raise ValueError("forecasts require forecast_issue_time")
            if self.forecast_issue_time > self.valid_time:
                raise ValueError("forecast issue time must not follow valid time")
        elif self.forecast_issue_time is not None:
            raise ValueError("forecast_issue_time requires a forecast dataset")
        if self.value is None:
            if not variable.nullable:
                raise ValueError("variable does not allow missing values")
        else:
            if variable.data_type == "integer" and type(self.value) is not int:
                raise ValueError("integer variable requires an integer value")
            if variable.minimum is not None and self.value < variable.minimum:
                raise ValueError("value is below variable minimum")
            if variable.maximum is not None and self.value > variable.maximum:
                raise ValueError("value is above variable maximum")
        return self

class Bundle(Contract):
    dataset: Dataset
    observations: list[Observation]

    @model_validator(mode="after")
    def validate_records(self):
        ids = set()
        for index, observation in enumerate(self.observations):
            try:
                if observation.id in ids:
                    raise ValueError("duplicate observation ID")
                observation.validate_against(self.dataset)
                ids.add(observation.id)
            except ValueError as error:
                raise ValueError(f"observations[{index}] ({observation.id}): {error}") from error
        return self
