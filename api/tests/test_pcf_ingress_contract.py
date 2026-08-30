import json
import math
import re
import unittest
from pathlib import Path

from order_forecast.api import machine_auth


ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = ROOT / "order_forecast/contracts/pcf/ingress-auth-v1.json"
PROJECTION_CONTRACT_PATH = ROOT / "order_forecast/contracts/pcf/projection-evidence-v1.json"
D5_CONTRACT_PATH = ROOT / "order_forecast/contracts/pcf/d5-deadline-v1.json"
RETENTION_CONTRACT_PATH = ROOT / "order_forecast/contracts/pcf/retention-v1.json"


class PcfIngressContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    def test_decisions_and_google_identity_match_runtime_constants(self):
        self.assertEqual(
            self.contract["decisions"],
            {
                "D1": "existing_fastapi_service",
                "D6": "cloudflare_tunnel_with_origin_machine_auth",
                "D2": "cloud_tasks_http_push_oidc",
            },
        )
        task = self.contract["identities"][0]
        self.assertEqual(task["audience"], machine_auth.TASK_EVENT_AUDIENCE)
        self.assertEqual(task["principal"], machine_auth.TASK_DISPATCHER_EMAIL)

    def test_transport_is_bounded_below_provider_limits(self):
        transport = self.contract["transport"]
        self.assertEqual(transport["retentionDays"], 31)
        self.assertLessEqual(transport["bodyBytesMax"], transport["providerTaskBytesMax"])
        self.assertLess(transport["bodyBytesMax"], transport["providerTaskBytesMax"])
        self.assertEqual(transport["retry"]["maxAttempts"], -1)
        self.assertEqual(transport["retry"]["maxRetryDuration"], "0s")
        self.assertEqual(
            transport["producerServiceAccount"],
            "pcf-event-producer@routespark-1f47d.iam.gserviceaccount.com",
        )
        self.assertEqual(
            transport["iam"],
            {
                "producerProjectRoles": [
                    "roles/datastore.user",
                    "roles/logging.logWriter",
                ],
                "producerQueueRole": "roles/cloudtasks.enqueuer",
                "dispatcherActAsRole": "roles/iam.serviceAccountUser",
                "queueScopedEnqueuer": True,
            },
        )
        self.assertEqual(
            transport["queueOverrides"],
            {
                "method": "POST",
                "scheme": "HTTPS",
                "host": "api.routespark.pro",
                "path": "/internal/v1/pcf/events",
                "contentType": "application/json",
                "oidcServiceAccount": machine_auth.TASK_DISPATCHER_EMAIL,
                "oidcAudience": machine_auth.TASK_EVENT_AUDIENCE,
            },
        )
        ages = transport["ageControls"]
        self.assertLess(ages["operatingSloSeconds"], ages["warningSeconds"])
        self.assertLess(ages["warningSeconds"], ages["effectsOffSeconds"])
        self.assertLess(ages["effectsOffSeconds"], ages["expiryIncidentSeconds"])
        self.assertLess(ages["expiryIncidentSeconds"], transport["retentionDays"] * 86400)

    def test_every_remote_endpoint_has_one_identity_and_no_token_granted_capabilities(self):
        identities = self.contract["identities"]
        self.assertEqual(len(identities), 4)
        self.assertEqual(len({entry["path"] for entry in identities}), len(identities))
        self.assertFalse(self.contract["failClosed"]["capabilitiesFromToken"])
        self.assertFalse(self.contract["failClosed"]["internalApiDefaultEnabled"])
        self.assertTrue(self.contract["failClosed"]["authenticateBeforeBody"])
        self.assertTrue(self.contract["failClosed"]["rejectCookies"])
        self.assertTrue(self.contract["failClosed"]["rejectFirebaseTokens"])
        self.assertFalse(self.contract["failClosed"]["internalCorsPreflight"])

    def test_checked_in_contract_contains_no_private_or_stripe_secret_material(self):
        source = CONTRACT_PATH.read_text(encoding="utf-8")
        forbidden = (
            "BEGIN PRIVATE KEY",
            "client_secret",
            "CF-Access-Client-Secret",
            "STRIPE_SECRET_KEY",
            "STRIPE_WEBHOOK_SECRET",
        )
        for value in forbidden:
            self.assertNotIn(value, source)

    def test_checked_in_cloudflare_and_network_policy_prove_the_selected_network_path(self):
        tunnel = (ROOT / "k8s/overlays/lab/cloudflared-configmap.yaml").read_text(
            encoding="utf-8"
        )
        policy = (ROOT / "k8s/base/web-api/networkpolicy.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("hostname: api.routespark.pro", tunnel)
        self.assertIn("service: http://web-api:8000", tunnel)
        self.assertIn("app.kubernetes.io/name: cloudflared", policy)
        self.assertNotIn("ipBlock:", policy.split("---", 1)[0])
        api_config = (ROOT / "k8s/overlays/lab/web-api-configmap.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn('PCF_INTERNAL_API_ENABLED: "false"', api_config)


class PcfProjectionEvidenceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(PROJECTION_CONTRACT_PATH.read_text(encoding="utf-8"))

    def test_d3_identity_is_dedicated_and_does_not_claim_path_scoped_iam(self):
        projection = self.contract["projectionBoundary"]
        self.assertEqual(
            projection["firebaseIdentity"],
            "routespark-pcf-projection-fb@routespark-1f47d.iam.gserviceaccount.com",
        )
        self.assertEqual(projection["projectRoles"], ["roles/datastore.user"])
        self.assertFalse(projection["firestorePathScopeClaimed"])
        self.assertNotIn("roles/owner", json.dumps(projection))
        self.assertNotIn("roles/editor", json.dumps(projection))

    def test_signer_and_cleaner_have_separate_narrow_storage_contracts(self):
        storage = self.contract["storageBoundaries"]
        self.assertEqual(storage["claimUrlSigner"]["permissions"], ["storage.objects.get"])
        self.assertFalse(storage["claimUrlSigner"]["privateKeyFile"])
        self.assertEqual(
            storage["exactCleaner"]["permissions"],
            ["storage.objects.get", "storage.objects.delete"],
        )
        self.assertTrue(storage["exactCleaner"]["generationPreconditionRequired"])
        self.assertTrue(storage["exactCleaner"]["prefixOrBulkDeleteForbidden"])

    def test_mac_worker_has_no_firebase_postgres_or_storage_credential(self):
        worker = self.contract["workerBoundary"]
        self.assertFalse(worker["firebaseCredential"])
        self.assertFalse(worker["postgresCredential"])
        self.assertFalse(worker["storageCredential"])

    def test_external_evidence_remains_authoritative_and_fail_closed(self):
        evidence = self.contract["evidenceAuthority"]
        self.assertEqual(
            evidence["root"],
            "/Volumes/Extreme SSD/routespark_offload/ocr-agent-review",
        )
        self.assertEqual(evidence["failureMode"], "block before projection or cleanup")
        self.assertFalse(evidence["overwriteExistingPackage"])
        self.assertFalse(evidence["postgresIsEvidenceAuthority"])
        self.assertFalse(evidence["clusterStorageIsEvidenceAuthority"])
        self.assertFalse(evidence["automaticEvidenceDeletion"])
        mount = evidence["clusterReadMount"]
        self.assertEqual(
            mount["root"], "/mnt/archive/pcf/pcf_effect_evidence"
        )
        self.assertEqual(mount["mode"], "readOnly")
        self.assertEqual(mount["requiredFor"], ["projecting", "cleaning"])
        self.assertFalse(mount["writableByCluster"])
        self.assertEqual(mount["missingBehavior"], "block_current_effect_step")
        publication = evidence["publication"]
        self.assertEqual(publication["macMount"], "/Volumes/RouteSparkArchive")
        self.assertTrue(publication["requiredBeforeResultSubmission"])
        self.assertFalse(publication["networkClientInWorker"])
        self.assertFalse(publication["remoteOrPrefixDelete"])

        verifier = self.contract["verificationBoundary"]
        self.assertEqual(
            verifier["identity"],
            "routespark-pcf-verifier@routespark-1f47d.iam.gserviceaccount.com",
        )
        self.assertEqual(
            verifier["projectRoles"],
            ["roles/datastore.viewer", "roles/storage.objectViewer"],
        )
        self.assertFalse(verifier["writeOrDelete"])

    def test_canonical_copy_has_a_distinct_generation_fenced_identity(self):
        storage = self.contract["storageBoundaries"]
        projector = storage["canonicalProjector"]
        self.assertEqual(
            projector["permissions"],
            ["storage.objects.get", "storage.objects.create", "storage.objects.update"],
        )
        self.assertTrue(projector["sourceGenerationPreconditionRequired"])
        self.assertTrue(projector["destinationGenerationPreconditionRequired"])
        self.assertTrue(projector["prefixOrBulkWriteForbidden"])
        self.assertNotEqual(
            projector["identity"],
            storage["exactCleaner"]["identity"],
        )
        self.assertNotEqual(
            projector["identity"],
            storage["claimUrlSigner"]["identity"],
        )

    def test_projection_allowlist_uses_the_live_plural_pcf_collection(self):
        allowlist = self.contract["projectionBoundary"]["applicationWriteAllowlist"]
        self.assertIn("routes/{route}/pcfs/{delivery}", allowlist)
        self.assertIn(
            "routes/{route}/pcfs/{delivery}/containers/{container}",
            allowlist,
        )
        self.assertNotIn("routes/{route}/pcf/{delivery}", allowlist)
        self.assertNotIn(
            "routes/{route}/pcf/{delivery}/containers/{container}",
            allowlist,
        )

    def test_rollout_is_effect_disabled_and_does_not_use_review_route_as_canary(self):
        rollout = self.contract["rollout"]
        self.assertFalse(rollout["productionResourcesExist"])
        self.assertFalse(rollout["effectsDefaultEnabled"])
        self.assertEqual(rollout["routeOrder"], ["988200", "989262", "widen"])
        self.assertEqual(rollout["route900000"], "observe_only")

    def test_projection_contract_contains_no_secret_or_stripe_material(self):
        source = PROJECTION_CONTRACT_PATH.read_text(encoding="utf-8")
        for value in (
            "BEGIN PRIVATE KEY",
            "serviceAccountKey.json",
            "STRIPE_SECRET_KEY",
            "STRIPE_WEBHOOK_SECRET",
        ):
            self.assertNotIn(value, source)


class PcfD5DeadlineContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(D5_CONTRACT_PATH.read_text(encoding="utf-8"))

    def test_evidence_gate_uses_distinct_successful_deliveries(self):
        evidence = self.contract["evidence"]
        self.assertGreaterEqual(
            evidence["collectionDays"], evidence["requiredCollectionDays"]
        )
        self.assertGreaterEqual(
            evidence["successfulDeliveryCount"],
            evidence["requiredSuccessfulDeliveries"],
        )
        self.assertTrue(evidence["evidenceReady"])
        self.assertEqual(evidence["rawPrefixLineCount"], evidence["attemptCount"])
        self.assertRegex(evidence["rawPrefixSha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(evidence["summarySha256"], r"^[0-9a-f]{64}$")

    def test_every_deadline_is_derived_from_the_frozen_rule(self):
        p99 = self.contract["effectiveClaimToTerminalP99Seconds"]
        deadlines = self.contract["hardDeadlineSeconds"]
        self.assertEqual(set(p99), set(deadlines))
        for band, measured in p99.items():
            expected = math.ceil(max(600, 1.5 * measured["value"]))
            self.assertEqual(deadlines[band], expected)

    def test_lease_and_signed_url_lifetime_preserve_deadline_margin(self):
        lease = self.contract["lease"]
        self.assertEqual(lease["renewableLeaseSeconds"], 4 * lease["heartbeatSeconds"])
        self.assertGreaterEqual(
            lease["signedUrlMinimumLifetimeAfterClaimSeconds"],
            max(self.contract["hardDeadlineSeconds"].values()) + 300,
        )

    def test_queue_age_target_does_not_claim_unmeasured_legacy_evidence(self):
        queue_age = self.contract["queueAge"]
        self.assertEqual(queue_age["normalClaimableTransitionToClaimSloSeconds"], 15)
        self.assertIn("does not measure enqueue-to-claim", queue_age["measurementStatus"])

    def test_d5_contract_contains_no_route_delivery_or_secret_material(self):
        source = D5_CONTRACT_PATH.read_text(encoding="utf-8")
        self.assertIsNone(re.search(r'"route"\s*:', source))
        self.assertIsNone(re.search(r'"delivery"\s*:', source))
        self.assertNotIn("STRIPE_", source)
        self.assertNotIn("BEGIN PRIVATE KEY", source)


class PcfRetentionContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(RETENTION_CONTRACT_PATH.read_text(encoding="utf-8"))

    def test_horizons_exceed_replay_lateness_and_rollback_windows(self):
        windows = self.contract["governingWindowsDays"]
        tombstone_days = self.contract["itemsCurrent"]["tombstoneMinimumDays"]
        terminal_days = self.contract["dueWork"]["terminalMinimumDays"]
        self.assertGreater(tombstone_days, windows["managedTransportReplay"])
        self.assertGreater(tombstone_days, windows["rollbackProof"])
        self.assertGreater(terminal_days, windows["managedTransportReplay"])
        self.assertGreater(terminal_days, windows["maximumNotificationLateness"])
        self.assertGreater(terminal_days, windows["rollbackProof"])

    def test_active_and_unresolved_work_can_never_be_pruned(self):
        never = set(self.contract["dueWork"]["neverPruneStatuses"])
        self.assertEqual(
            never,
            {
                "scheduled",
                "retryable",
                "blocked",
                "dispatching",
                "outcome_ambiguous",
                "dead_letter",
            },
        )
        self.assertTrue(
            self.contract["itemsCurrent"]["tombstonesLeaveActiveIndexesImmediately"]
        )
        self.assertTrue(
            self.contract["dueWork"]["terminalsLeaveClaimIndexesImmediately"]
        )

    def test_pruning_requires_checkpoints_backup_and_bounded_batches(self):
        item_guards = " ".join(self.contract["itemsCurrent"]["pruneRequires"])
        due_guards = " ".join(self.contract["dueWork"]["terminalPruneRequires"])
        self.assertIn("checkpoint", item_guards)
        self.assertIn("off-box backup", item_guards)
        self.assertIn("checkpoint", due_guards)
        self.assertIn("off-box backup", due_guards)
        pruner = self.contract["pruner"]
        self.assertEqual(pruner["batchSize"], 500)
        self.assertEqual(pruner["selection"], "FOR UPDATE SKIP LOCKED")
        self.assertFalse(pruner["deleteAllOrTruncate"])

    def test_hot_indexes_are_partial_and_partitioning_is_not_premature(self):
        indexes = self.contract["indexes"]
        self.assertIn("partial index", indexes["itemsActive"])
        self.assertIn("partial index", indexes["dueReady"])
        self.assertTrue(indexes["partitioning"].startswith("none initially"))

    def test_r14_does_not_expand_to_other_retention_systems(self):
        scope = self.contract["scope"]
        self.assertTrue(scope)
        self.assertTrue(all(value is False for value in scope.values()))


if __name__ == "__main__":
    unittest.main()
