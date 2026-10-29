"""iNaturalist API client for fetching observations."""

import logging
import re
import threading
from datetime import datetime, timedelta
from math import asin, ceil, cos, radians, sin, sqrt
from typing import Dict, Iterator, List, NamedTuple, Optional, Tuple

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyinaturalist import (
    Annotation,
    ClientSession,
    Observation,
    get_controlled_terms,
    get_observations_v2,
    get_projects,
    get_projects_by_id,
    get_taxa,
    get_taxa_autocomplete,
)

from app.services.errors import IntegrationConnectionError, IntegrationError

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

# The by-ID project endpoint takes at most this many IDs or slugs per request.
PROJECTS_BY_ID_BATCH_SIZE = 10
# The project search's id filter takes far more numeric IDs per request.
PROJECTS_SEARCH_BATCH_SIZE = 200
# A value that could be a project ID or slug. The endpoint rejects the whole
# request over one value that is neither (a space gives 422, a "/" 404).
PROJECT_ID_OR_SLUG = re.compile(r"^[A-Za-z0-9_-]+$")


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


# Typeahead lookups are interactive, so they fail fast instead of using
# pyinaturalist's default 10s timeout with 5 retries. The session keeps the
# default rate-limit file, so the daily request budget stays shared with pulls.
TYPEAHEAD_TIMEOUT_SECONDS = 5
# The typeahead shares pyinaturalist's per-host rate-limit bucket with pulls.
# pyinaturalist's default lets a request sleep up to 60 s for a slot; a search
# is interactive, so it fails fast with BucketFullException instead.
TYPEAHEAD_MAX_RATE_LIMIT_WAIT_SECONDS = 2
_typeahead = threading.local()


def _typeahead_session() -> ClientSession:
    # Thread-local like pyinaturalist's own session: callers run in worker threads.
    if not hasattr(_typeahead, "session"):
        session = ClientSession(max_retries=0, timeout=TYPEAHEAD_TIMEOUT_SECONDS)
        # pyinaturalist's retry config lists 5xx statuses, and with zero retries
        # urllib3 turns those into a RetryError that drops the status. Without
        # them, a 5xx reaches raise_for_status as an HTTPError that keeps it.
        session.retries = Retry(total=0, raise_on_status=False)
        session.mount("https://", HTTPAdapter(max_retries=session.retries))
        session.max_delay = TYPEAHEAD_MAX_RATE_LIMIT_WAIT_SECONDS
        _typeahead.session = session
    return _typeahead.session


def search_taxa(q: str) -> Dict:
    """iNaturalist taxa matching a typed query (public autocomplete endpoint)."""
    try:
        return get_taxa_autocomplete(q=q, session=_typeahead_session())
    except (requests.Timeout, requests.ConnectionError) as e:
        raise IntegrationConnectionError(f"iNaturalist did not respond to the taxa search: {e}") from e


# Saved taxa are looked up through the taxa search (`id` filter), which takes
# up to 500 IDs per request; the by-ID endpoint takes 30, so a config with
# hundreds of taxa ran past the typeahead's rate-limit bucket (5 per 5 s).
TAXA_BY_ID_BATCH_SIZE = 500


class INatRequestError(requests.HTTPError):
    """An iNaturalist API request failed; carries the message iNat returned.

    Subclasses requests.HTTPError so the request/response metadata that
    action_runner._handle_error reports keeps flowing through.
    """


class INatUnavailableError(IntegrationError):
    """iNaturalist didn't answer, dropped the connection, or kept answering with
    errors while the observations were being read.

    Not retried (its error_type is outside the runner's retryable set): the
    next scheduled pull starts from the same point, and a redelivery would only
    add load while iNaturalist is struggling.
    """
    error_type = "provider_unavailable"
    default_title = "iNaturalist is unavailable"


class INatIncompleteBatchError(INatRequestError):
    """A by-ID taxa batch whose completeness cannot be established (paged,
    missing count metadata, or malformed), so IDs absent from it must not be
    treated as missing."""


class TaxaLookup(NamedTuple):
    found: Dict[int, Dict]  # taxon id -> full upstream record
    missing: List[int]      # requested ids conclusively absent, in request order


def _taxa_batches(numeric: List[int]) -> Iterator[Tuple[List[int], Dict]]:
    """Yield (batch, response) for each by-ID taxa request."""
    for start in range(0, len(numeric), TAXA_BY_ID_BATCH_SIZE):
        batch = numeric[start:start + TAXA_BY_ID_BATCH_SIZE]
        try:
            # is_active="any": a saved taxon may have been inactivated since (a
            # taxonomy change); the search hides those by default, the by-ID
            # endpoint didn't.
            response = get_taxa(
                id=batch, is_active="any", per_page=TAXA_BY_ID_BATCH_SIZE,
                session=_typeahead_session(),
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            raise IntegrationConnectionError(f"iNaturalist did not respond to the taxa lookup: {e}") from e
        yield batch, response


def get_taxa_by_ids(ids: List[str]) -> List[Dict]:
    """Taxa records for saved taxon IDs (public endpoint), so the portal can label
    saved values. IDs iNat doesn't know are simply absent; values that can't be
    a taxon ID (free text) are not sent."""
    numeric = [int(i) for i in ids if i.isdigit()]
    taxa = []
    for _, response in _taxa_batches(numeric):
        taxa.extend(response.get("results", []))
    return taxa


def lookup_taxa_by_ids(numeric_ids: List[int]) -> TaxaLookup:
    """Like get_taxa_by_ids, but tells IDs iNat confirmed absent from IDs whose
    status is unknown. A batch counts as complete only when total_results equals
    the number of results returned and fits in one page; anything else raises
    INatIncompleteBatchError rather than reporting a partial answer."""
    found: Dict[int, Dict] = {}
    missing: List[int] = []
    for batch, response in _taxa_batches(numeric_ids):
        results = response.get("results")
        total = response.get("total_results")
        if (
            not isinstance(results, list)
            or not isinstance(total, int) or isinstance(total, bool)
            or total != len(results)
            or total > TAXA_BY_ID_BATCH_SIZE
        ):
            raise INatIncompleteBatchError(
                f"iNaturalist returned an incomplete taxa batch (total_results={total!r}, "
                f"results={len(results) if isinstance(results, list) else type(results).__name__})"
            )
        batch_found: Dict[int, Dict] = {}
        for taxon in results:
            taxon_id = taxon.get("id") if isinstance(taxon, dict) else None
            if not isinstance(taxon_id, int) or isinstance(taxon_id, bool):
                raise INatIncompleteBatchError("iNaturalist returned a taxon record without an integer id")
            batch_found[taxon_id] = taxon
        found.update(batch_found)
        missing.extend(i for i in batch if i not in batch_found)
    return TaxaLookup(found, missing)


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


def _projects_by_value(values: List[str], **session) -> Dict[str, Dict]:
    """Project records for saved values (numeric IDs or slugs), keyed by the
    saved spelling. Values that can't be an ID or slug are not sent."""
    lookups = [v for v in values if PROJECT_ID_OR_SLUG.match(v)]
    ids = [v for v in lookups if v.isdigit()]
    slugs = [v for v in lookups if not v.isdigit()]
    projects = []
    # The project search's id filter takes many IDs per request; slugs only go
    # through the by-ID endpoint (the search ignores a slug filter).
    for start in range(0, len(ids), PROJECTS_SEARCH_BATCH_SIZE):
        batch = ids[start:start + PROJECTS_SEARCH_BATCH_SIZE]
        projects.extend(get_projects(id=batch, per_page=len(batch), **session).get("results", []))
    for start in range(0, len(slugs), PROJECTS_BY_ID_BATCH_SIZE):
        batch = slugs[start:start + PROJECTS_BY_ID_BATCH_SIZE]
        projects.extend(get_projects_by_id(batch, **session).get("results", []))
    by_key = {}
    for project in projects:
        if project.get("id") is not None:
            by_key[str(project["id"])] = project
        if project.get("slug"):
            by_key[project["slug"].lower()] = project
    return {v: by_key.get(v) or by_key[v.lower()] for v in values if v in by_key or v.lower() in by_key}


def find_existing_projects(values: List[str]) -> List[str]:
    """The saved project values (numeric IDs or slugs) that exist on iNaturalist,
    in their original order and spelling. Values that can't be an ID or slug
    are treated as unknown without being sent."""
    found = _projects_by_value(values)
    return [v for v in values if v in found]


def get_projects_by_values(values: List[str]) -> Dict[str, Dict]:
    """Project records for saved values, keyed by the saved spelling, so the
    portal can label them (the resolve contract). Interactive, so it uses the
    fail-fast typeahead session; values iNat doesn't know are absent."""
    try:
        return _projects_by_value(values, session=_typeahead_session())
    except (requests.Timeout, requests.ConnectionError) as e:
        raise IntegrationConnectionError(f"iNaturalist did not respond to the project lookup: {e}") from e


def _call_inat(**params) -> Dict:
    """Call the iNat observations endpoint, surfacing iNat's own error messages.

    requests raises a bare "422 Client Error: ... for url: <query string>", which
    hides the actual reason (unknown project slug, bad taxon id, ...). iNat puts
    that in the response body, so lift it into the exception message.

    Every message names iNaturalist, so the activity log shows at a glance that
    the failure is on iNaturalist's side rather than Gundi's.
    """
    try:
        return get_observations_v2(**params)
    except requests.HTTPError as e:
        detail = _error_detail(e.response)
        status = f"{e.response.status_code} {e.response.reason or ''}".strip() if e.response is not None else str(e)
        message = f"iNaturalist answered with an error when asked for observations: {status}"
        raise INatRequestError(
            f"{message} - {detail}" if detail else message,
            request=e.request, response=e.response,
        ) from e
    except requests.exceptions.RetryError as e:
        # pyinaturalist's session retries 5xx answers itself; this is what is left
        # when every retry got one.
        raise INatUnavailableError("it kept answering with errors when asked for observations") from e
    except requests.Timeout as e:
        # Before ConnectionError: a connect timeout is both.
        raise INatUnavailableError("no answer in time when asked for observations") from e
    except requests.ConnectionError as e:
        # A read timeout that outlasts the session's retries arrives as a
        # ConnectionError wrapping urllib3's MaxRetryError: tell it apart by its reason.
        if isinstance(getattr(e.args[0] if e.args else None, "reason", None), urllib3.exceptions.TimeoutError):
            raise INatUnavailableError("no answer in time when asked for observations") from e
        raise INatUnavailableError("the connection dropped when asking for observations") from e
    except requests.exceptions.ChunkedEncodingError as e:
        # The connection closed partway through an answer.
        raise INatUnavailableError("the connection dropped when asking for observations") from e


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
