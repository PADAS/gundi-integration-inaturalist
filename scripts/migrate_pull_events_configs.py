"""One-off migration: rewrite stored pull_events configs into the list shapes the
current schema expects.

`taxa` (PR #30), `annotations` and `quality_grade` (PR #29) used to be stored as
strings. The runner still coerces the old shapes, but the portal validates
stored data against the registered schema, so a legacy config can't be edited
until it is rewritten. Only those three fields are touched; every other field
is written back exactly as stored.

Run it AFTER registering the new pull_events schema: the Gundi API validates the
PATCHed config against the registered schema, so lists are rejected before then.

Dry run by default; --apply writes. Every original config is saved to the backup
file before anything is written.

    python -m scripts.migrate_pull_events_configs --type-slug inaturalist
    python -m scripts.migrate_pull_events_configs --type-slug inaturalist --apply

Auth: GUNDI_TOKEN (a bearer token for an account that can edit these
integrations) if set, otherwise the runner's OAuth client credentials.
GUNDI_API_BASE_URL selects the environment.
"""
import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

import httpx
import pydantic

from app.actions.configurations import PullEventsConfig

ACTION = "pull_events"
MIGRATED_FIELDS = ("taxa", "annotations", "quality_grade")


def normalize(data: dict) -> Optional[dict]:
    """The config with MIGRATED_FIELDS in their current shape, or None if
    nothing changes. Raises pydantic.ValidationError if the runner itself would
    reject the config; those need a manual fix."""
    parsed = PullEventsConfig.parse_obj(data)
    new = dict(data)
    for name in MIGRATED_FIELDS:
        if name not in data:
            continue
        value = getattr(parsed, name)
        if value is None:
            # Absent and null mean the same to the runner, but the schema types
            # these fields as arrays, so null would fail validation.
            del new[name]
        elif name == "annotations":
            new[name] = [row.dict() for row in value]
        else:
            new[name] = value
    return new if new != data else None


@dataclass
class Report:
    unchanged: List[str] = field(default_factory=list)
    migrated: List[str] = field(default_factory=list)
    invalid: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)


async def _get_all(client: httpx.AsyncClient, url: str, params: dict) -> List[dict]:
    items = []
    while url:
        response = await client.get(url, params=params)
        response.raise_for_status()
        body = response.json()
        if isinstance(body, list):
            return items + body
        items.extend(body["results"])
        url, params = body.get("next"), None  # `next` already carries the query
    return items


async def migrate(client: httpx.AsyncClient, base_url: str, type_slug: str,
                  apply: bool, backup_path: str, out=sys.stdout) -> Report:
    api = f"{base_url.rstrip('/')}/v2"
    types = await _get_all(client, f"{api}/integrations/types/", {"value": type_slug})
    if len(types) != 1:
        raise SystemExit(f"Expected one integration type {type_slug!r}, found {len(types)}.")
    integrations = await _get_all(client, f"{api}/integrations/", {"type": types[0]["id"]})

    report = Report()
    backup = []
    plans = []
    for integration in integrations:
        label = f"{integration['id']} ({integration.get('name')})"
        for config in integration.get("configurations") or []:
            if (config.get("action") or {}).get("value") != ACTION:
                continue
            data = config.get("data") or {}
            try:
                new = normalize(data)
            except pydantic.ValidationError as e:
                report.invalid.append(label)
                print(f"INVALID  {label}: needs a manual fix: {e.errors()}", file=out)
                continue
            if new is None:
                report.unchanged.append(label)
                continue
            backup.append({"integration_id": integration["id"], "configuration_id": config["id"], "data": data})
            plans.append((label, integration["id"], config["id"], new))
            changes = {k: (data.get(k), new.get(k)) for k in MIGRATED_FIELDS if data.get(k) != new.get(k)}
            print(f"MIGRATE  {label}: {json.dumps(changes)}", file=out)

    if plans:
        with open(backup_path, "w") as f:
            json.dump(backup, f, indent=2)
        print(f"Backed up {len(backup)} original config(s) to {backup_path}", file=out)

    for label, integration_id, configuration_id, new in plans:
        if not apply:
            report.migrated.append(label)
            continue
        response = await client.patch(
            f"{api}/integrations/{integration_id}/",
            json={"configurations": [{"id": configuration_id, "data": new}]},
        )
        if response.is_success:
            report.migrated.append(label)
        else:
            report.failed.append(label)
            print(f"FAILED   {label}: {response.status_code} {response.text[:500]}", file=out)

    verb = "Migrated" if apply else "Would migrate"
    print(
        f"{verb} {len(report.migrated)}, unchanged {len(report.unchanged)}, "
        f"invalid {len(report.invalid)}, failed {len(report.failed)}"
        + ("" if apply else " (dry run; pass --apply to write)"),
        file=out,
    )
    return report


async def _auth_headers() -> dict:
    if token := os.environ.get("GUNDI_TOKEN"):
        return {"Authorization": f"Bearer {token}"}
    from gundi_client_v2 import GundiClient
    gundi = GundiClient()
    try:
        return await gundi.get_auth_header()
    finally:
        await gundi.close()


async def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--type-slug", required=True, help="Integration type value, e.g. inaturalist")
    parser.add_argument("--base-url", default=os.environ.get("GUNDI_API_BASE_URL"))
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry run)")
    parser.add_argument(
        "--backup",
        default=f"pull_events_configs_backup_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json",
    )
    args = parser.parse_args()
    if not args.base_url:
        parser.error("--base-url or GUNDI_API_BASE_URL is required")

    async with httpx.AsyncClient(headers=await _auth_headers(), timeout=30) as client:
        report = await migrate(client, args.base_url, args.type_slug, args.apply, args.backup)
    sys.exit(1 if report.failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
