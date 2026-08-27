from typing import Optional, List, Dict, Literal
import json
import pydantic
from pyinaturalist.constants import QUALITY_GRADES
from .core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions


class AnnotationFilter(pydantic.BaseModel):
    term: str = pydantic.Field(
        ...,
        title="Annotation term",
        description="iNaturalist controlled term ID (e.g. '22' for Evidence of Presence).",
    )
    values: List[str] = pydantic.Field(
        default_factory=list,
        title="Allowed values",
        description="Controlled value IDs required for this term (e.g. '24' for Organism).",
    )


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: pydantic.SecretStr = pydantic.Field(..., title = "iNaturalist API Key",
                                  description = "API key generated from iNat",
                                  format="password")
    
class PullEventsConfig(PullActionConfiguration):

    event_type: Optional[str] = pydantic.Field("inat_observation", title="Event type",
        description="The event type to use in the returned event data.")

    event_prefix: str = pydantic.Field("iNat: ", title="Event prefix",
        description = "A string to prefix to the observed species to set a title when creating the event.  Default: 'iNat: '")

    days_to_load: int = FieldWithUIOptions(
        3,
        title = "Default number of days to load",
        ge=1,
        le=7,
        ui_options=UIOptions(
            widget="range",  # This will be rendered ad a range slider
        ),
        description="The number of days of data to load from iNaturalist.  If the integration state contains a last_run value, this parameter will be ignored and data will be loaded since the last_run value.")

    bounding_box: Optional[str] = pydantic.Field(title = "Bounding box for search area.  Of the format [ne_latitude, ne_longitude, sw_latitude, sw_longitude]")

    projects: Optional[List[str]] = pydantic.Field(title = "Project IDs",
        description="List of project IDs to pull from iNaturalist.")
    
    taxa: Optional[str] = pydantic.Field(title = "Taxa IDs",
        description="Comma-separated list of iNaturalist taxa IDs for which to load observations (e.g. '12345, 67890').")
    
    quality_grade: Optional[List[Literal["casual", "needs_id", "research"]]] = pydantic.Field(
        None,
        title="Quality Grade",
        description="If present, only observations that have one of the selected quality grades will be included.",
    )

    annotations: Optional[List[AnnotationFilter]] = pydantic.Field(
        None,
        title="Annotations",
        description=(
            "Annotation filters. Each row selects a controlled term and the values required "
            "for that term — e.g. term 22 (Evidence of Presence) with values 24 (Organism) "
            "or 25 (Scat). All terms listed must be present on an observation; within a "
            "term, all listed values must be present. Legacy JSON-string configs "
            '(e.g. {"22": ["24", "25"]}) are still accepted.'
        ),
    )

    include_photos: Optional[bool] = pydantic.Field(True, title="Include photos",
        description = "Whether or not to include the photos from iNaturalist observations.  Default: True")

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "taxa",
            "projects",
            "quality_grade",
            "days_to_load",
            "annotations",
            "bounding_box",
            "event_type",
            "event_prefix",
            "include_photos",
            "run_on_schedule",
        ],
    )

    @pydantic.validator("taxa", pre=True, always=True)
    def coerce_taxa_list_to_str(cls, v):
        if isinstance(v, list):
            return ",".join(str(t) for t in v if t)
        return v

    # Temporary validator to cope with a limitation in Gundi Portal.
    @pydantic.validator("event_type", "event_prefix", always=True)
    def validate_region_code(cls, v, values):
        if 'any' == str(v).lower():
            return None
        return v

    @pydantic.validator("annotations", pre=True, always=True)
    def coerce_annotations(cls, v):
        # Legacy configs stored this as a JSON-encoded string (or raw dict) of
        # {term: [values]}; coerce both into the structured row shape.
        if v is None:
            return None
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
            try:
                v = json.loads(v)
            except Exception:
                raise ValueError(f"Could not parse json: {v}")
        if isinstance(v, dict):
            return [
                {"term": str(term), "values": [str(value) for value in (values or [])]}
                for term, values in v.items()
            ]
        return v

    @pydantic.validator("quality_grade", pre=True, always=True)
    def validate_quality_grade(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.split(",")
        elif not isinstance(v, (list, tuple, set)):
            # A mis-typed scalar becomes a one-item list so it reaches the message
            # naming the value, rather than pydantic's "value is not a valid list".
            v = [v]
        if not v:
            return v
        # iNat accepts space/case variants (e.g. "needs id"); normalize like pyinaturalist does.
        # Drop only unset entries (None / blank strings); anything else must validate,
        # so a wrong value like 0 raises instead of silently disabling the filter.
        normalized = [str(g).strip().lower().replace(" ", "_") for g in v if g is not None and str(g).strip()]
        invalid = [g for g in normalized if g not in QUALITY_GRADES]
        if invalid:
            raise ValueError(
                f"Invalid quality_grade value(s) {invalid}; must be one of {QUALITY_GRADES}."
            )
        return normalized

    @pydantic.validator("bounding_box", always=True)
    def validate_bounding_box(cls, v):
        if(not v):
            return None
        v = json.loads(v)
        if(len(v) != 4):
            raise ValueError("Did not receive four values in bounding box configuration.")
        for i in range(0,4):
            try:
                v[i] = float(v[i])
            except:
                raise ValueError(f"Could not parse bounding box values {v}.")

        if(v[0] < -90 or v[0] > 90):
            raise ValueError(f"NE Latitude {v[0]} must be between -90 and 90")

        if(v[1] < -180 or v[1] > 180):
            raise ValueError(f"NE Longitude {v[1]} must be between -180 and 180")
        
        if(v[2] < -90 or v[2] > 90):
            raise ValueError(f"SW Latitude {v[2]} must be between -90 and 90")

        if(v[3] < -180 or v[3] > 180):
            raise ValueError(f"SW Longitude {v[2]} must be between -180 and 180")
        
        if(v[0] <= v[2]):
            raise ValueError(f"NE Latitude {v[0]} must be greater than SW Latitude {v[2]}")
        
        if(v[1] <= v[3]):
            raise ValueError(f"NE Longitude {v[1]} must be greater than SW Longitude {v[3]}")
        
        return v

    @property
    def annotations_dict(self) -> Optional[Dict[str, List[str]]]:
        """The {term: [values]} shape the datasource's annotation matcher consumes."""
        if not self.annotations:
            return None
        return {f.term: f.values for f in self.annotations}

    class Config:
        schema_extra = {
            "examples": [
                {
                    "": 47.5218082,
                    "longitude": -122.3864506,
                    "distance": 30,
                    "num_days_default": 1
                }
            ],
            "required": ["bounding_box", "days_to_load"]
        }