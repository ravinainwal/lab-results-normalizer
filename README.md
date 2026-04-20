# lab-normalizer

Internal Python service for normalizing lab results from multiple source systems
into a canonical schema, exposed over an HTTP API.

## Architecture

```
source payload ──► LabResultParser ──► LabResult (canonical) ──► LabResultRepository ──► HTTP API
   (CSV/JSON/…)      (per-source)         domain/models.py          storage/                api/
```

The two extension seams are:

| Seam                       | Contract                              | Current impl                  |
|----------------------------|---------------------------------------|-------------------------------|
| Input format               | `ingestion.base.LabResultParser`      | `ingestion.acme_json` (JSON)  |
| Persistence                | `storage.base.LabResultRepository`    | `storage.memory` (in-process) |

Adding a new source system = implement a `LabResultParser` subclass and register
it in `ingestion/registry.py`. No changes to storage or API are required.

> **Note:** the in-memory repository is for dev/tests only. A database-backed
> implementation must be wired in before production deployment — `create_app()`
> in `api/app.py` accepts a `repository=` argument for exactly this swap.

## Canonical model

See `lab_normalizer/domain/models.py::LabResult`. Key fields: `patient_id`,
`test_code` (LOINC where available), `test_name`, `value_numeric` /
`value_text`, `unit`, `reference_range`, `status`, `collected_at`,
`reported_at`, `source_system`.

## Running locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
uvicorn lab_normalizer.api.app:app --reload
```

Interactive API docs: http://localhost:8000/docs

### Try the end-to-end path

```bash
# Ingest the sample Acme Labs JSON feed
curl -F "file=@samples/acme_labs.json" http://localhost:8000/v1/ingest/acme_json

# Query normalized results for a patient
curl "http://localhost:8000/v1/results?patient_id=P-42"
```

## API

| Method | Path                       | Description                                  |
|--------|----------------------------|----------------------------------------------|
| GET    | `/v1/health`               | Liveness check                               |
| GET    | `/v1/sources`              | List registered source-system parsers        |
| POST   | `/v1/ingest/{source}`      | Upload a raw payload for normalization       |
| GET    | `/v1/results`              | Query normalized results (filter + paginate) |
| GET    | `/v1/results/{result_id}`  | Fetch one normalized result                  |

Query filters on `/v1/results`: `patient_id`, `test_code`, `source_system`,
`collected_from`, `collected_to`, `limit`, `offset`.

## Tests

```bash
pytest
```

## Configuration

Environment variables (prefix `LABNORM_`): `LOG_LEVEL`, `MAX_UPLOAD_BYTES`.
