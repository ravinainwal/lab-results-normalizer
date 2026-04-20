from __future__ import annotations

import io
import json

from fastapi.testclient import TestClient


def _upload(client: TestClient, source: str, payload: bytes):
    return client.post(
        f"/v1/ingest/{source}",
        files={"file": ("data.json", io.BytesIO(payload), "application/json")},
    )


def test_health(client: TestClient) -> None:
    resp = client.get("/v1/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_sources_lists_registered_parsers(client: TestClient) -> None:
    resp = client.get("/v1/sources")
    assert resp.status_code == 200
    assert "acme_json" in resp.json()


def test_ingest_then_query_end_to_end(client: TestClient, acme_payload: bytes) -> None:
    resp = _upload(client, "acme_json", acme_payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source_system"] == "acme_json"
    assert body["ingested"] == 4
    assert body["rejected_count"] == 0
    assert body["rejected"] == []
    assert len(body["result_ids"]) == 4

    # Query all results for patient P-42.
    resp = client.get("/v1/results", params={"patient_id": "P-42"})
    assert resp.status_code == 200
    page = resp.json()
    assert page["total"] == 2
    assert all(item["patient_id"] == "P-42" for item in page["items"])
    # Public schema must not leak the raw vendor payload.
    assert "raw" not in page["items"][0]
    # And it must expose the comparator so analytics keeps bounded-value fidelity.
    assert "value_comparator" in page["items"][0]


def test_bounded_value_surfaces_comparator_over_api(
    client: TestClient, acme_payload: bytes
) -> None:
    _upload(client, "acme_json", acme_payload).raise_for_status()
    resp = client.get("/v1/results", params={"test_code": "3016-3"})  # TSH LOINC
    assert resp.status_code == 200
    [tsh] = resp.json()["items"]
    assert tsh["value_numeric"] == 0.01
    assert tsh["value_comparator"] == "<"
    assert tsh["value_text"] is None


# ---------- reingestion ----------

def test_reingesting_same_file_is_idempotent(
    client: TestClient, acme_payload: bytes
) -> None:
    """The original complaint: uploading the same file twice must not duplicate rows."""
    first = _upload(client, "acme_json", acme_payload).json()
    second = _upload(client, "acme_json", acme_payload).json()

    # Same IDs on both uploads.
    assert sorted(first["result_ids"]) == sorted(second["result_ids"])

    # Repository still has the original count, not double.
    resp = client.get("/v1/results", params={"limit": 1000})
    page = resp.json()
    assert page["total"] == 4

    # Each source_record_id appears exactly once.
    src_ids = [item["source_record_id"] for item in page["items"]]
    assert sorted(src_ids) == ["R-1001", "R-1002", "R-2001", "R-2002"]


def test_reingest_with_correction_overwrites(
    client: TestClient, acme_payload: bytes
) -> None:
    """A corrected record (same source_record_id, new values) must replace the prior version."""
    _upload(client, "acme_json", acme_payload).raise_for_status()

    corrected = json.loads(acme_payload)
    # Find R-2001 (Hemoglobin, was preliminary with 13.1) and correct it.
    for item in corrected["results"]:
        if item["id"] == "R-2001":
            item["status"] = "C"
            item["observation"]["value"] = 13.4

    _upload(
        client, "acme_json", json.dumps(corrected).encode()
    ).raise_for_status()

    resp = client.get("/v1/results", params={"limit": 1000})
    page = resp.json()
    assert page["total"] == 4  # still four, not five

    hgb = next(i for i in page["items"] if i["source_record_id"] == "R-2001")
    assert hgb["status"] == "corrected"
    assert hgb["value_numeric"] == 13.4


# ---------- partial success ----------

def test_partial_success_returns_200_with_rejections(client: TestClient) -> None:
    """One bad record in a batch must not reject the rest; the response reports both."""
    payload = json.dumps(
        {
            "results": [
                {
                    "id": "ok-1",
                    "patient": {"mrn": "P1"},
                    "test": {"code": "GLU", "name": "Glucose"},
                    "observation": {"value": 5.4},
                    "collected": "2026-04-10T08:00:00Z",
                },
                {
                    "id": "bad-1",
                    # Missing patient.mrn
                    "test": {"code": "GLU", "name": "Glucose"},
                    "observation": {"value": 5.4},
                    "collected": "2026-04-10T08:00:00Z",
                },
                {
                    "id": "ok-2",
                    "patient": {"mrn": "P2"},
                    "test": {"code": "GLU", "name": "Glucose"},
                    "observation": {"value": 6.0},
                    "collected": "2026-04-10T09:00:00Z",
                },
            ]
        }
    ).encode()

    resp = _upload(client, "acme_json", payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["ingested"] == 2
    assert body["rejected_count"] == 1
    [rejected] = body["rejected"]
    assert rejected["index"] == 1
    assert rejected["source_record_id"] == "bad-1"
    assert "patient.mrn" in rejected["error"]

    # Only the valid records made it into storage.
    page = client.get("/v1/results", params={"limit": 1000}).json()
    assert page["total"] == 2
    src_ids = sorted(item["source_record_id"] for item in page["items"])
    assert src_ids == ["ok-1", "ok-2"]


def test_all_records_bad_still_returns_200(client: TestClient) -> None:
    """Envelope valid but every record failed -> 200 with ingest=0 and full rejection list."""
    payload = json.dumps(
        {
            "results": [
                {"id": "bad-1", "test": {"code": "GLU", "name": "Glucose"}},
                {"id": "bad-2", "test": {"code": "GLU", "name": "Glucose"}},
            ]
        }
    ).encode()
    resp = _upload(client, "acme_json", payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ingested"] == 0
    assert body["rejected_count"] == 2


# ---------- envelope-level failures ----------

def test_ingest_unknown_source_returns_404(client: TestClient) -> None:
    resp = _upload(client, "nope", b"{}")
    assert resp.status_code == 404


def test_ingest_malformed_payload_returns_400(client: TestClient) -> None:
    resp = _upload(client, "acme_json", b"not json")
    assert resp.status_code == 400
    assert "invalid JSON" in resp.json()["detail"]


def test_get_missing_result_returns_404(client: TestClient) -> None:
    resp = client.get("/v1/results/does-not-exist")
    assert resp.status_code == 404
