import datetime

import pytest

from app.services.webhooks import (
    _validate_diagnostic_url,
    forward_payload_to_diagnostic_url,
)


def _addr_infos(*addresses):
    # getaddrinfo returns 5-tuples; only sockaddr[0] is read by the validator.
    return [(None, None, None, None, (addr, 0)) for addr in addresses]


@pytest.fixture
def mock_getaddrinfo(mocker):
    def _patch(*addresses):
        loop = mocker.MagicMock()
        loop.getaddrinfo = mocker.AsyncMock(return_value=_addr_infos(*addresses))
        mocker.patch("app.services.webhooks.asyncio.get_running_loop", return_value=loop)
        return loop
    return _patch


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "169.254.169.254",  # cloud metadata endpoint
        "10.0.0.5",
        "::1",
    ],
)
async def test_validate_diagnostic_url_blocks_private_addresses(mock_getaddrinfo, address):
    mock_getaddrinfo(address)

    with pytest.raises(ValueError, match="private or reserved address"):
        await _validate_diagnostic_url("https://evil.example.com/hook")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "address",
    [
        "::ffff:127.0.0.1",
        "::ffff:169.254.169.254",  # cloud metadata via an IPv4-mapped AAAA record
        "::ffff:10.0.0.5",
    ],
)
async def test_validate_diagnostic_url_blocks_ipv4_mapped_ipv6(mock_getaddrinfo, address):
    """An IPv4-mapped IPv6 address is outside every blocked network literally,
    but connects to the mapped IPv4 address, so it must be normalized first."""
    mock_getaddrinfo(address)

    with pytest.raises(ValueError, match="private or reserved address"):
        await _validate_diagnostic_url("https://evil.example.com/hook")


@pytest.mark.asyncio
async def test_validate_diagnostic_url_allows_public_address(mock_getaddrinfo):
    mock_getaddrinfo("93.184.216.34")

    await _validate_diagnostic_url("https://example.com/hook")


@pytest.mark.asyncio
async def test_validate_diagnostic_url_rejects_non_https(mock_getaddrinfo):
    with pytest.raises(ValueError, match="is not allowed"):
        await _validate_diagnostic_url("http://example.com/hook")


@pytest.mark.asyncio
async def test_forward_payload_posts_payload_with_metadata(mocker, mock_getaddrinfo):
    """Guards the whole happy path: it runs inside a broad `except Exception`,
    so any error here (e.g. datetime.UTC on Python 3.10) silently disables it."""
    mock_getaddrinfo("93.184.216.34")
    mock_client = mocker.MagicMock()
    mock_client.post = mocker.AsyncMock(return_value=mocker.MagicMock())
    mocker.patch("app.services.webhooks._get_diagnostic_client", return_value=mock_client)

    await forward_payload_to_diagnostic_url(
        destination_url="https://example.com/hook",
        integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
        json_content={"device": "abc123"},
    )

    assert mock_client.post.called
    _, kwargs = mock_client.post.call_args
    body = kwargs["json"]
    assert body["device"] == "abc123"
    metadata = body["__gundi_diagnostic_metadata"]
    assert metadata["integration_id"] == "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0"
    # Must be a valid RFC 3339 timestamp -- no doubled UTC designator.
    assert not metadata["received_at"].endswith("+00:00Z")
    parsed = datetime.datetime.fromisoformat(metadata["received_at"])
    assert parsed.tzinfo is not None


@pytest.mark.asyncio
async def test_forward_payload_wraps_non_dict_content(mocker, mock_getaddrinfo):
    mock_getaddrinfo("93.184.216.34")
    mock_client = mocker.MagicMock()
    mock_client.post = mocker.AsyncMock(return_value=mocker.MagicMock())
    mocker.patch("app.services.webhooks._get_diagnostic_client", return_value=mock_client)

    await forward_payload_to_diagnostic_url(
        destination_url="https://example.com/hook",
        integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
        json_content=[1, 2, 3],
    )

    _, kwargs = mock_client.post.call_args
    assert kwargs["json"]["payload"] == [1, 2, 3]


@pytest.mark.asyncio
async def test_forward_payload_does_not_post_to_blocked_address(mocker, mock_getaddrinfo):
    mock_getaddrinfo("169.254.169.254")
    mock_client = mocker.MagicMock()
    mock_client.post = mocker.AsyncMock(return_value=mocker.MagicMock())
    mocker.patch("app.services.webhooks._get_diagnostic_client", return_value=mock_client)

    await forward_payload_to_diagnostic_url(
        destination_url="https://evil.example.com/hook",
        integration_id="779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0",
        json_content={"device": "abc123"},
    )

    assert not mock_client.post.called
