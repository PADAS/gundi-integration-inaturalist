import io
import json

import httpx
import pydantic
import pytest

from scripts.migrate_pull_events_configs import migrate, normalize

BASE = "https://api.test"


@pytest.mark.parametrize("data,expected", [
    (
        {"days_to_load": 3, "taxa": "12345, 67890", "bounding_box": "[1, 1, 0, 0]"},
        {"days_to_load": 3, "taxa": ["12345", "67890"], "bounding_box": "[1, 1, 0, 0]"},
    ),
    (
        {"days_to_load": 3, "projects": ["1"], "annotations": '{"22": ["24"]}', "quality_grade": "research,needs_id"},
        {"days_to_load": 3, "projects": ["1"], "annotations": [{"term": "22", "values": ["24"]}],
         "quality_grade": ["research", "needs_id"]},
    ),
    # Empty legacy values are dropped rather than written as null.
    ({"days_to_load": 3, "projects": ["1"], "taxa": "", "annotations": ""}, {"days_to_load": 3, "projects": ["1"]}),
])
def test_normalize_rewrites_legacy_shapes_and_nothing_else(data, expected):
    assert normalize(data) == expected


def test_normalize_leaves_current_configs_alone():
    data = {"days_to_load": 3, "taxa": ["1"], "bounding_box": "[1, 1, 0, 0]",
            "annotations": [{"term": "22", "values": ["24"]}], "quality_grade": ["research"]}
    assert normalize(data) is None


def test_normalize_raises_on_configs_the_runner_rejects():
    with pytest.raises(pydantic.ValidationError):
        normalize({"days_to_load": 3, "taxa": "leopard"})


def _api(configs):
    """A fake Gundi API holding one iNat integration per config; records PATCHes."""
    patches = []

    def handler(request: httpx.Request):
        path = request.url.path
        if path == "/v2/integrations/types/":
            assert request.url.params["value"] == "inaturalist"
            return httpx.Response(200, json={"results": [{"id": "type-1"}], "next": None})
        if path == "/v2/integrations/" and request.method == "GET":
            assert request.url.params["type"] == "type-1"
            return httpx.Response(200, json={"next": None, "results": [
                {"id": f"int-{i}", "name": f"iNat {i}", "configurations": [
                    {"id": f"auth-{i}", "action": {"value": "auth"}, "data": {"api_key": "secret"}},
                    {"id": f"cfg-{i}", "action": {"value": "pull_events"}, "data": data},
                ]}
                for i, data in enumerate(configs)
            ]})
        if request.method == "PATCH":
            patches.append((path, json.loads(request.content)))
            return httpx.Response(200, json={})
        raise AssertionError(f"unexpected {request.method} {path}")

    return httpx.AsyncClient(transport=httpx.MockTransport(handler)), patches


CONFIGS = [
    {"days_to_load": 3, "taxa": "1,2", "bounding_box": "[1, 1, 0, 0]"},  # legacy
    {"days_to_load": 3, "taxa": ["1"], "bounding_box": "[1, 1, 0, 0]"},  # current
    {"days_to_load": 3, "taxa": "leopard"},  # invalid
]


@pytest.mark.asyncio
async def test_dry_run_writes_nothing_but_backs_up(tmp_path):
    client, patches = _api(CONFIGS)
    backup = tmp_path / "backup.json"

    report = await migrate(client, BASE, "inaturalist", apply=False, backup_path=str(backup), out=io.StringIO())

    assert patches == []
    assert report.migrated == ["int-0 (iNat 0)"]
    assert report.unchanged == ["int-1 (iNat 1)"]
    assert report.invalid == ["int-2 (iNat 2)"]
    assert json.loads(backup.read_text()) == [
        {"integration_id": "int-0", "configuration_id": "cfg-0", "data": CONFIGS[0]},
    ]


@pytest.mark.asyncio
async def test_apply_patches_only_the_pull_events_configuration(tmp_path):
    client, patches = _api(CONFIGS)

    report = await migrate(client, BASE, "inaturalist", apply=True,
                           backup_path=str(tmp_path / "b.json"), out=io.StringIO())

    assert report.migrated == ["int-0 (iNat 0)"] and report.failed == []
    assert patches == [("/v2/integrations/int-0/", {"configurations": [
        {"id": "cfg-0", "data": {"days_to_load": 3, "taxa": ["1", "2"], "bounding_box": "[1, 1, 0, 0]"}},
    ]})]
