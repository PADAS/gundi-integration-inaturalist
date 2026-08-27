"""iNaturalist API client for fetching observations."""

import logging
from datetime import datetime, timedelta
from math import asin, ceil, cos, radians, sin, sqrt
from typing import Dict, List, Optional, Tuple

import requests
from pyinaturalist import (
    Annotation,
    Observation,
    get_controlled_terms,
    get_observations_v2,
    get_projects,
)

logger = logging.getLogger(__name__)

OBSERVATION_FIELDS = [
    "observed_on", "created_at", "id", "captive", "obscured", "place_guess",
    "quality_grade", "species_guess", "updated_at", "uri", "photos", "user",
    "location", "place_ids", "taxon",
    "photos.large_url", "photos.url",
    "taxon.id", "taxon.rank", "taxon.name", "taxon.preferred_common_name",
    "taxon.wikipedia_url", "taxon.conservation_status",
    "user.id", "user.name", "user.login",
    "annotations.controlled_attribute_id", "annotations.controlled_value_id",
]

# Reference-action project search: one page of nearest projects; the portal's
# combobox allows free text for anything beyond it (truncated=True signals the cap).
PROJECTS_PAGE_SIZE = 200
MAX_PROJECT_SEARCH_RADIUS_KM = 500.0


def _match_annotations_to_config(annotations: List[Annotation], config: Dict) -> bool:
    """Check that the observation has all annotation term/value pairs required by config."""
    annot_map = {}
    for annotation in annotations:
        key = str(annotation.term)
        if key not in annot_map:
            annot_map[key] = []
        annot_map[key].append(str(annotation.value))

    for term, values in config.items():
        term_str = str(term)
        if term_str not in annot_map:
            return False
        allowed = annot_map[term_str]
        for value in values:
            if str(value) not in allowed:
                return False
    return True


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    lat1, lng1, lat2, lng2 = map(radians, (lat1, lng1, lat2, lng2))
    a = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lng2 - lng1) / 2) ** 2
    return 2 * 6371.0 * asin(sqrt(a))


def bbox_to_search_circle(bounding_box: List[float]) -> Tuple[float, float, float]:
    """Center and covering radius (km) of a [ne_lat, ne_lng, sw_lat, sw_lng] box.

    iNat project search is point+radius, not box, so the box is approximated by
    the circle through its corners. Radius is clamped to [1, 500] km: a floor so
    a tiny box still finds its local projects, a cap because a continent-sized
    search circle returns noise anyway (the portal keeps free text for the rest).
    """
    ne_lat, ne_lng, sw_lat, sw_lng = bounding_box[:4]
    center_lat = (ne_lat + sw_lat) / 2
    center_lng = (ne_lng + sw_lng) / 2
    radius_km = _haversine_km(center_lat, center_lng, ne_lat, ne_lng)
    return center_lat, center_lng, min(max(radius_km, 1.0), MAX_PROJECT_SEARCH_RADIUS_KM)


def list_controlled_terms() -> List[Dict]:
    """All iNaturalist annotation controlled terms (public endpoint, no auth)."""
    response = get_controlled_terms()
    return response.get("results", []) if isinstance(response, dict) else []


def search_projects_near(lat: float, lng: float, radius_km: float) -> Dict:
    """Nearest-first iNaturalist projects within radius_km of a point (public endpoint)."""
    return get_projects(
        lat=lat, lng=lng, radius=radius_km, order_by="distance", per_page=PROJECTS_PAGE_SIZE
    )


class INatRequestError(requests.HTTPError):
    """An iNaturalist API request failed; carries the message iNat returned.

    Subclasses requests.HTTPError so the request/response metadata that
    action_runner._handle_error reports keeps flowing through.
    """


def _error_detail(response) -> Optional[str]:
    """Pull iNat's own error message out of an error response body."""
    if response is None:
        return None
    try:
        body = response.json()
    except ValueError:
        body = None
    if isinstance(body, dict):
        messages = [
            str(err["message"]) for err in body.get("errors") or []
            if isinstance(err, dict) and err.get("message")
        ]
        if messages:
            return "; ".join(messages)[:500]
    text = (response.text or "").strip()
    if text.startswith("<"):  # HTML error page from iNat or a proxy; nothing quotable
        return None
    return text[:500] or None


def _call_inat(**params) -> Dict:
    """Call the iNat observations endpoint, surfacing iNat's own error messages.

    requests raises a bare "422 Client Error: ... for url: <query string>", which
    hides the actual reason (unknown project slug, bad taxon id, ...). iNat puts
    that in the response body, so lift it into the exception message.
    """
    try:
        return get_observations_v2(**params)
    except requests.HTTPError as e:
        detail = _error_detail(e.response)
        raise INatRequestError(
            f"{e} - {detail}" if detail else str(e),
            request=e.request, response=e.response,
        ) from e


TAXA_BATCH_SIZE = 100
INAT_PAGE_SIZE = 200
# iNat rejects pagination past 10,000 results (page * per_page > 10,000 -> 500),
# so windows deeper than that are re-queried with updated_since advanced instead.
INAT_MAX_RESULTS_PER_QUERY = 10_000
MAX_PAGES_PER_WINDOW = INAT_MAX_RESULTS_PER_QUERY // INAT_PAGE_SIZE


def _get_observations_for_taxa_batch(
    base_params: Dict,
    taxa_batch: Optional[str],
    annotations: Optional[Dict],
    fields: str,
) -> Dict[int, Observation]:
    params = {**base_params}
    if taxa_batch:
        params["taxon_id"] = taxa_batch

    observation_map = {}
    updated_since = params["updated_since"]
    while True:
        window_params = {**params, "updated_since": updated_since}
        count_params = {**window_params, "page": 1, "per_page": 0}
        inat_count = (_call_inat(**count_params).get("total_results") or 0)
        pages = min(ceil(inat_count / INAT_PAGE_SIZE), MAX_PAGES_PER_WINDOW) if inat_count else 0

        last_updated_at = None
        for page in range(1, pages + 1):
            logger.debug("Loading page %s of %s from iNaturalist", page, pages)
            response = _call_inat(**{**window_params, "page": page, "per_page": INAT_PAGE_SIZE, "fields": fields})
            observations = Observation.from_json_list(response)
            logger.info("Loaded %s observations from iNaturalist before annotation filters.", len(observations))
            for o in observations:
                if o.updated_at:
                    last_updated_at = o.updated_at
                if annotations:
                    if _match_annotations_to_config(o.annotations, annotations):
                        observation_map[o.id] = o
                else:
                    observation_map[o.id] = o

        if inat_count <= INAT_MAX_RESULTS_PER_QUERY:
            break
        if last_updated_at is None:
            logger.warning(
                "iNaturalist reported %s results for updated_since=%s but returned no "
                "observations; stopping to avoid an infinite query loop.",
                inat_count, updated_since,
            )
            break
        if last_updated_at == updated_since:
            # 10,000+ observations share this exact timestamp; the overflow within it
            # is unreachable (iNat can't paginate past 10k), but the rest of the
            # backlog is — step just past the timestamp and keep going.
            logger.warning(
                "The first %s results for updated_since=%s all share that updated_at "
                "timestamp; advancing the cursor by 1s to continue past it. Any further "
                "observations with that exact timestamp are beyond iNaturalist's "
                "pagination limit and will be skipped.",
                INAT_MAX_RESULTS_PER_QUERY, updated_since,
            )
            updated_since = last_updated_at + timedelta(seconds=1)
            continue
        logger.info(
            "Window matched %s observations (over the %s pagination limit); "
            "continuing from updated_since=%s.",
            inat_count, INAT_MAX_RESULTS_PER_QUERY, last_updated_at,
        )
        updated_since = last_updated_at

    return observation_map


def get_observations(
    since: datetime,
    *,
    bounding_box: Optional[List[float]] = None,
    taxa: Optional[str] = None,
    projects: Optional[List[str]] = None,
    quality_grade: Optional[List[str]] = None,
    annotations: Optional[Dict] = None,
) -> Dict[int, Observation]:
    """
    Fetch observations from iNaturalist updated since the given datetime.

    Returns a dict mapping observation id -> Observation for all observations
    that match the filters (and annotation filter when annotations is set).
    """
    nelat = nelng = swlat = swlng = None
    if bounding_box and len(bounding_box) >= 4:
        nelat, nelng, swlat, swlng = bounding_box[:4]

    fields = ",".join(OBSERVATION_FIELDS)

    base_params = {
        "updated_since": since,
        "order_by": "updated_at",
        "order": "asc",
    }
    if projects is not None:
        base_params["project_id"] = projects
    if quality_grade is not None:
        base_params["quality_grade"] = quality_grade
    if nelat is not None and nelng is not None and swlat is not None and swlng is not None:
        base_params["nelat"] = nelat
        base_params["nelng"] = nelng
        base_params["swlat"] = swlat
        base_params["swlng"] = swlng

    taxa_ids = [t.strip() for t in taxa.split(",") if t.strip()] if taxa else []
    if taxa_ids:
        batches = [
            ",".join(taxa_ids[i:i + TAXA_BATCH_SIZE])
            for i in range(0, len(taxa_ids), TAXA_BATCH_SIZE)
        ]
    else:
        batches = [None]

    observation_map = {}
    for batch in batches:
        batch_results = _get_observations_for_taxa_batch(base_params, batch, annotations, fields)
        observation_map.update(batch_results)

    return observation_map
