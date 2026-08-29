import copy
import json
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi import FastAPI
from pydantic import ValidationError

from order_forecast.api.machine_auth import MachinePrincipal
from order_forecast.api.middleware.internal_machine_auth import InternalMachineAuthMiddleware
from order_forecast.api.pcf_worker_contract import (
    ClaimRequest,
    ContractValidationError,
    ResultRequest,
    canonical_result_identity,
    manifest_pages,
    validate_result_for_work,
)
from order_forecast.api.pcf_work_runtime import (
    ClaimGrant,
    IamV4ClaimUrlSigner,
    mint_signed_objects,
)
from order_forecast.api.routers import pcf_internal


ROUTE = "988200"
DELIVERY = "1806242572"
CONTAINER = "003400001234567890"
BUCKET = "routespark-1f47d.firebasestorage.app"
NOW = datetime(2026, 8, 29, 16, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[3]


def manifest():
    return {
        "containerCount": 1,
        "pageCount": 1,
        "objects": [
            {
                "containerCode": CONTAINER,
                "uploadPageNumber": 1,
                "objectPath": f"routes/{ROUTE}/unprocessed/{DELIVERY}_{CONTAINER}_page_1.jpg",
                "generation": "123456789",
                "size": 1200,
                "md5Hash": "YWJjZGVmMDEyMzQ1Njc4OQ==",
            }
        ],
    }


def ocr_context():
    return {
        "validatedLoadingDate": "08/01/2026",
        "containers": [
            {
                "containerCode": CONTAINER,
                "loadingDate": "08/01/2026",
                "ssccBypassUsed": False,
                "userVerifiedContainerCode": True,
                "containerCodeSource": "",
                "realSsccRecovered": False,
                "ssccRecoveryStatus": "",
            }
        ],
    }


def proposal():
    return {
        "route": ROUTE,
        "delivery": DELIVERY,
        "loadingDate": "08/01/2026",
        "identityVerification": {
            "deliveryNumber": {
                "value": DELIVERY,
                "status": "verified",
                "evidence": ["Printed delivery header and barcode agree."],
            },
            "loadingDate": {
                "value": "08/01/2026",
                "status": "verified",
                "evidence": ["Printed loading date and row arithmetic agree."],
            },
        },
        "deliveryAssignments": [
            {
                "deliveryNumber": DELIVERY,
                "loadingDate": "08/01/2026",
                "containerCodes": [CONTAINER],
                "status": "verified",
                "evidence": ["Container page has matching delivery identity."],
            }
        ],
        "confidence": 96,
        "confidenceTier": "high",
        "confidenceReasons": ["Every submitted row is legible."],
        "uncertainRows": [],
        "containers": [
            {
                "containerCode": CONTAINER,
                "pages": [
                    {
                        "pageNumber": 1,
                        "items": [
                            {
                                "product": "12345",
                                "bestBefore": "08/31/2026",
                                "days": "30*",
                                "isShortCoded": True,
                                "description": "TEST PRODUCT",
                            },
                            {
                                "product": "9191",
                                "bestBefore": "",
                                "days": "0",
                                "isShortCoded": False,
                                "description": "NO PRINTED DATE",
                            },
                        ],
                    }
                ],
            }
        ],
    }


def result_payload(*, token="t" * 43, work_id=None):
    work_id = work_id or uuid.uuid4()
    object_row = manifest()["objects"][0]
    return {
        "schemaVersion": 1,
        "workId": str(work_id),
        "attemptNumber": 1,
        "claimToken": token,
        "inputHashVersion": "pcf-work-input-v1",
        "inputHash": "a" * 64,
        "workerId": "pcf-macos-vision-01",
        "workerBuildVersion": "pcf-worker-2026.08.29",
        "ocrContractVersion": "pcf-agent-review-v1",
        "completedAt": "2026-08-29T16:01:00Z",
        "proposal": proposal(),
        "evidencePackage": {
            "route": ROUTE,
            "delivery": DELIVERY,
            "inputHashVersion": "pcf-work-input-v1",
            "inputHash": "a" * 64,
            "manifestSha256": "b" * 64,
            "manifestRelativePath": "evidence/manifest.json",
            "artifacts": [
                {
                    "role": "source_image",
                    "relativePath": "evidence/images/page_1.jpg",
                    "sha256": "c" * 64,
                    "sizeBytes": 1200,
                    "objectPath": object_row["objectPath"],
                    "generation": object_row["generation"],
                },
                {
                    "role": "before_payload",
                    "relativePath": "evidence/firebase-before.json",
                    "sha256": "d" * 64,
                    "sizeBytes": 20,
                    "objectPath": None,
                    "generation": None,
                },
                {
                    "role": "proposed_result",
                    "relativePath": "result.json",
                    "sha256": "e" * 64,
                    "sizeBytes": 30,
                    "objectPath": None,
                    "generation": None,
                },
                {
                    "role": "package_manifest",
                    "relativePath": "evidence/manifest.json",
                    "sha256": "b" * 64,
                    "sizeBytes": 40,
                    "objectPath": None,
                    "generation": None,
                },
            ],
        },
    }


class PcfWorkerContractTests(unittest.TestCase):
    def test_checked_in_result_schema_reuses_complete_proposal_and_contains_no_secrets(self):
        worker_path = ROOT / "order_forecast/contracts/pcf/worker-result-v1.schema.json"
        proposal_path = ROOT / "order_forecast/contracts/pcf/review-proposal-v1.schema.json"
        worker = json.loads(worker_path.read_text(encoding="utf-8"))
        review = json.loads(proposal_path.read_text(encoding="utf-8"))
        self.assertEqual(
            worker["properties"]["proposal"]["$ref"], "review-proposal-v1.schema.json"
        )
        self.assertIn("deliveryAssignments", review["required"])
        self.assertEqual(
            set(review["$defs"]["product"]["not"]["enum"]),
            {"22770", "32346", "37433", "52094", "52191", "52271", "53078"},
        )
        combined = worker_path.read_text(encoding="utf-8") + proposal_path.read_text(
            encoding="utf-8"
        )
        for forbidden in ("BEGIN PRIVATE KEY", "STRIPE_SECRET", "serviceAccountKey.json"):
            self.assertNotIn(forbidden, combined)

    def test_claim_contract_is_strict_and_bounded(self):
        request = ClaimRequest.model_validate(
            {
                "workerId": "pcf-macos-vision-01",
                "requiredCapability": "macos_vision",
                "supportedResultSchemaVersions": [1],
                "waitSeconds": 30,
            }
        )
        self.assertEqual(request.wait_seconds, 30)
        with self.assertRaises(ValidationError):
            ClaimRequest.model_validate(
                {
                    "workerId": "pcf-macos-vision-01",
                    "requiredCapability": "macos_vision",
                    "supportedResultSchemaVersions": [1],
                    "waitSeconds": "30",
                }
            )

    def test_manifest_is_exact_generation_bound_and_canonical(self):
        pages = manifest_pages(manifest(), route=ROUTE, delivery=DELIVERY, expected_bucket=BUCKET)
        self.assertEqual(pages[0]["storageGeneration"], "123456789")
        escaped = manifest()
        escaped["objects"][0]["objectPath"] = escaped["objects"][0]["objectPath"].replace(ROUTE, "989262")
        with self.assertRaisesRegex(ContractValidationError, "escaped"):
            manifest_pages(escaped, route=ROUTE, delivery=DELIVERY, expected_bucket=BUCKET)
        zero_generation = manifest()
        zero_generation["objects"][0]["generation"] = "0"
        with self.assertRaisesRegex(ContractValidationError, "generation"):
            manifest_pages(zero_generation, route=ROUTE, delivery=DELIVERY, expected_bucket=BUCKET)

    def test_manifest_normalizes_valid_transport_order_without_changing_identity(self):
        unordered = manifest()
        second = copy.deepcopy(unordered["objects"][0])
        second.update(
            {
                "uploadPageNumber": 2,
                "objectPath": f"routes/{ROUTE}/unprocessed/{DELIVERY}_{CONTAINER}_page_2.jpg",
                "generation": "123456790",
            }
        )
        unordered["objects"] = [second, unordered["objects"][0]]
        unordered["pageCount"] = 2
        pages = manifest_pages(
            unordered, route=ROUTE, delivery=DELIVERY, expected_bucket=BUCKET
        )
        self.assertEqual([page["pageNumber"] for page in pages], [1, 2])

    def test_complete_result_validates_identity_arithmetic_stars_omissions_and_evidence(self):
        raw = result_payload()
        request = ResultRequest.model_validate(raw)
        self.assertEqual(
            validate_result_for_work(
                request,
                route=ROUTE,
                delivery=DELIVERY,
                input_hash="a" * 64,
                manifest=manifest(),
                ocr_context=ocr_context(),
                expected_bucket=BUCKET,
            ),
            96,
        )
        bad_days = copy.deepcopy(raw)
        bad_days["proposal"]["containers"][0]["pages"][0]["items"][0]["days"] = "29*"
        with self.assertRaisesRegex(ContractValidationError, "arithmetic"):
            validate_result_for_work(
                ResultRequest.model_validate(bad_days),
                route=ROUTE,
                delivery=DELIVERY,
                input_hash="a" * 64,
                manifest=manifest(),
                ocr_context=ocr_context(),
                expected_bucket=BUCKET,
            )
        omitted = copy.deepcopy(raw)
        omitted["proposal"]["containers"][0]["pages"][0]["items"][0]["product"] = "22770"
        with self.assertRaisesRegex(ContractValidationError, "omitted SAP"):
            validate_result_for_work(
                ResultRequest.model_validate(omitted),
                route=ROUTE,
                delivery=DELIVERY,
                input_hash="a" * 64,
                manifest=manifest(),
                ocr_context=ocr_context(),
                expected_bucket=BUCKET,
            )
        wrong_queue_date = copy.deepcopy(raw)
        wrong_queue_date["proposal"]["loadingDate"] = "08/02/2026"
        wrong_queue_date["proposal"]["identityVerification"]["loadingDate"]["value"] = "08/02/2026"
        wrong_queue_date["proposal"]["deliveryAssignments"][0]["loadingDate"] = "08/02/2026"
        with self.assertRaisesRegex(ContractValidationError, "frozen queue input"):
            validate_result_for_work(
                ResultRequest.model_validate(wrong_queue_date),
                route=ROUTE,
                delivery=DELIVERY,
                input_hash="a" * 64,
                manifest=manifest(),
                ocr_context=ocr_context(),
                expected_bucket=BUCKET,
            )

    def test_evidence_rejects_path_escape_and_missing_exact_source_generation(self):
        escaped = result_payload()
        escaped["evidencePackage"]["artifacts"][1]["relativePath"] = "../before.json"
        with self.assertRaisesRegex(ContractValidationError, "unsafe"):
            validate_result_for_work(
                ResultRequest.model_validate(escaped),
                route=ROUTE,
                delivery=DELIVERY,
                input_hash="a" * 64,
                manifest=manifest(),
                ocr_context=ocr_context(),
                expected_bucket=BUCKET,
            )
        wrong_generation = result_payload()
        wrong_generation["evidencePackage"]["artifacts"][0]["generation"] = "99"
        with self.assertRaisesRegex(ContractValidationError, "exactly cover"):
            validate_result_for_work(
                ResultRequest.model_validate(wrong_generation),
                route=ROUTE,
                delivery=DELIVERY,
                input_hash="a" * 64,
                manifest=manifest(),
                ocr_context=ocr_context(),
                expected_bucket=BUCKET,
            )

    def test_result_identity_never_retains_or_hashes_formatting_around_claim_token(self):
        first = result_payload(token="x" * 43)
        second = result_payload(token="y" * 43, work_id=uuid.UUID(first["workId"]))
        first_hash, first_retained = canonical_result_identity(first)
        second_hash, second_retained = canonical_result_identity(second)
        self.assertEqual(first_hash, second_hash)
        self.assertNotIn("claimToken", first_retained)
        self.assertNotIn("claimToken", second_retained)


class _FakeSigner:
    def __init__(self):
        self.calls = []

    def sign_get(self, **kwargs):
        self.calls.append(kwargs)
        return (
            f"https://storage.googleapis.com/{kwargs['bucket']}/{kwargs['object_path']}?"
            f"generation={kwargs['generation']}&X-Goog-Signature=abc"
        )


class PcfClaimSignerTests(unittest.TestCase):
    def test_minting_passes_only_exact_manifest_identity_and_deadline_margin(self):
        grant = ClaimGrant(
            work_id=uuid.uuid4(),
            attempt_number=1,
            claim_token="t" * 43,
            lease_expires_at=NOW + timedelta(seconds=120),
            hard_deadline_at=NOW + timedelta(seconds=600),
            route=ROUTE,
            delivery=DELIVERY,
            input_hash_version="pcf-work-input-v1",
            input_hash="a" * 64,
            source_queue_path=f"pcf_processing_queue/{ROUTE}/queue/{DELIVERY}",
            manifest=manifest(),
            ocr_context=ocr_context(),
        )
        signer = _FakeSigner()
        signed = mint_signed_objects(grant, signer)
        self.assertEqual(len(signed), 1)
        self.assertEqual(signer.calls[0]["bucket"], BUCKET)
        self.assertEqual(signer.calls[0]["generation"], "123456789")
        self.assertEqual(signer.calls[0]["expires_at"], NOW + timedelta(seconds=900))

    def test_real_signer_fails_before_adc_without_exact_dedicated_configuration(self):
        with patch.dict("os.environ", {}, clear=True), patch("google.auth.default") as adc:
            with self.assertRaisesRegex(RuntimeError, "not configured"):
                IamV4ClaimUrlSigner()
        adc.assert_not_called()


class _EndpointConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class PcfWorkerEndpointTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _app(*, enabled=True):
        app = FastAPI()
        app.include_router(pcf_internal.router)
        app.add_middleware(InternalMachineAuthMiddleware, enabled=enabled)
        return app

    async def _request(self, method, path, payload, principal, **patches):
        transport = httpx.ASGITransport(app=self._app())
        with patch(
            "order_forecast.api.middleware.internal_machine_auth.authenticate_internal_headers",
            return_value=principal,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                return await client.request(
                    method,
                    path,
                    content=json.dumps(payload),
                    headers={
                        "authorization": "Bearer verified-by-test",
                        "content-type": "application/json",
                    },
                )

    async def test_disabled_boundary_rejects_claim_before_body_or_database(self):
        transport = httpx.ASGITransport(app=self._app(enabled=False))
        with patch.object(pcf_internal, "get_pcf_pg_connection") as get_conn:
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post("/internal/v1/pcf/claims", content=b"not-json")
        self.assertEqual(response.status_code, 404)
        get_conn.assert_not_called()

    async def test_claim_returns_committed_exact_generation_urls_and_no_store(self):
        conn = _EndpointConnection()
        firestore_client = object()
        grant = ClaimGrant(
            work_id=uuid.uuid4(),
            attempt_number=1,
            claim_token="t" * 43,
            lease_expires_at=NOW + timedelta(seconds=120),
            hard_deadline_at=NOW + timedelta(seconds=600),
            route=ROUTE,
            delivery=DELIVERY,
            input_hash_version="pcf-work-input-v1",
            input_hash="a" * 64,
            source_queue_path=f"pcf_processing_queue/{ROUTE}/queue/{DELIVERY}",
            manifest=manifest(),
            ocr_context=ocr_context(),
        )
        principal = MachinePrincipal(
            "pcf-macos-vision-01", "routespark_machine_jwt", "pcf.claim", "test-kid"
        )
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ), patch.object(
            pcf_internal, "get_firestore", return_value=firestore_client
        ), patch.object(pcf_internal, "wait_for_claim", return_value=grant), patch.object(
            pcf_internal, "disarm_exact_queue_ttl"
        ) as disarm, patch.object(
            pcf_internal, "build_claim_url_signer", return_value=_FakeSigner()
        ):
            response = await self._request(
                "POST",
                "/internal/v1/pcf/claims",
                {
                    "workerId": "pcf-macos-vision-01",
                    "requiredCapability": "macos_vision",
                    "supportedResultSchemaVersions": [1],
                    "waitSeconds": 0,
                },
                principal,
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "no-store")
        body = response.json()
        self.assertEqual(body["claimToken"], "t" * 43)
        self.assertEqual(body["signedObjects"][0]["generation"], "123456789")
        self.assertEqual(body["ocrContext"], ocr_context())
        self.assertIn("generation=123456789", body["signedObjects"][0]["url"])
        disarm.assert_called_once_with(
            firestore_client,
            source_queue_path=grant.source_queue_path,
            route=ROUTE,
            delivery=DELIVERY,
        )

    async def test_signer_failure_revokes_unreturned_claim_and_is_retryable(self):
        conn = _EndpointConnection()
        firestore_client = object()
        grant = ClaimGrant(
            uuid.uuid4(), 1, "t" * 43, NOW + timedelta(seconds=120),
            NOW + timedelta(seconds=600), ROUTE, DELIVERY, "pcf-work-input-v1",
            "a" * 64, f"pcf_processing_queue/{ROUTE}/queue/{DELIVERY}", manifest(),
            ocr_context(),
        )
        principal = MachinePrincipal(
            "pcf-macos-vision-01", "routespark_machine_jwt", "pcf.claim", "test-kid"
        )
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ), patch.object(
            pcf_internal, "get_firestore", return_value=firestore_client
        ), patch.object(pcf_internal, "wait_for_claim", return_value=grant), patch.object(
            pcf_internal, "disarm_exact_queue_ttl"
        ), patch.object(
            pcf_internal, "build_claim_url_signer", side_effect=RuntimeError("unavailable")
        ), patch.object(pcf_internal, "release_unmaterialized_claim", return_value=True) as release:
            response = await self._request(
                "POST",
                "/internal/v1/pcf/claims",
                {
                    "workerId": "pcf-macos-vision-01",
                    "requiredCapability": "macos_vision",
                    "supportedResultSchemaVersions": [1],
                    "waitSeconds": 0,
                },
                principal,
            )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers["retry-after"], "10")
        self.assertNotIn("t" * 43, response.text)
        release.assert_called_once()
        self.assertEqual(conn.commits, 1)

    async def test_ttl_disarm_failure_revokes_unreturned_claim_before_signing(self):
        conn = _EndpointConnection()
        firestore_client = object()
        grant = ClaimGrant(
            uuid.uuid4(), 1, "t" * 43, NOW + timedelta(seconds=120),
            NOW + timedelta(seconds=600), ROUTE, DELIVERY, "pcf-work-input-v1",
            "a" * 64, f"pcf_processing_queue/{ROUTE}/queue/{DELIVERY}", manifest(),
            ocr_context(),
        )
        principal = MachinePrincipal(
            "pcf-macos-vision-01", "routespark_machine_jwt", "pcf.claim", "test-kid"
        )
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ), patch.object(
            pcf_internal, "get_firestore", return_value=firestore_client
        ), patch.object(pcf_internal, "wait_for_claim", return_value=grant), patch.object(
            pcf_internal, "disarm_exact_queue_ttl", side_effect=RuntimeError("unavailable")
        ), patch.object(pcf_internal, "build_claim_url_signer") as signer, patch.object(
            pcf_internal, "release_unmaterialized_claim", return_value=True
        ) as release:
            response = await self._request(
                "POST",
                "/internal/v1/pcf/claims",
                {
                    "workerId": "pcf-macos-vision-01",
                    "requiredCapability": "macos_vision",
                    "supportedResultSchemaVersions": [1],
                    "waitSeconds": 0,
                },
                principal,
            )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers["retry-after"], "10")
        self.assertNotIn("t" * 43, response.text)
        signer.assert_not_called()
        release.assert_called_once()
        self.assertEqual(release.call_args.kwargs["reason"], "queue_ttl_disarm_failed")
        self.assertEqual(conn.commits, 1)

    async def test_result_endpoint_preserves_typed_conflict(self):
        raw = result_payload()
        work_id = raw["workId"]
        conn = _EndpointConnection()
        principal = MachinePrincipal(
            "pcf-macos-vision-01", "routespark_machine_jwt", "pcf.result", "test-kid"
        )
        conflict = __import__(
            "order_forecast.api.pcf_work_runtime", fromlist=["WorkApiError"]
        ).WorkApiError(409, "lease_expired")
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ), patch.object(pcf_internal, "accept_result", side_effect=conflict):
            response = await self._request(
                "PUT", f"/internal/v1/pcf/work/{work_id}/result", raw, principal
            )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json(), {"error": {"code": "lease_expired"}})
        self.assertEqual(conn.rollbacks, 1)


if __name__ == "__main__":
    unittest.main()
