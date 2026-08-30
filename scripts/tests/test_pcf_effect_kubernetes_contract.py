from __future__ import annotations

import json
from pathlib import Path

import yaml

from order_forecast.api.pcf_effect_control import parse_effect_control
from order_forecast.api.pcf_runtime_control import parse_runtime_control


ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "k8s/base/pcf-effects"
CONFIG = ROOT / "k8s/overlays/lab/pcf-effects-configmap.yaml"


def documents(path: Path):
    return [item for item in yaml.safe_load_all(path.read_text(encoding="utf-8")) if item]


def test_effect_deployment_is_physically_and_logically_disabled():
    deployment = documents(BASE / "deployment.yaml")[0]
    assert deployment["spec"]["replicas"] == 0
    pod = deployment["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    container = pod["containers"][0]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["allowPrivilegeEscalation"] is False
    evidence = next(volume for volume in pod["volumes"] if volume["name"] == "pcf-evidence")
    assert evidence["hostPath"] == {
        "path": "/mnt/archive/pcf/pcf_effect_evidence",
        "type": "Directory",
    }
    evidence_mount = next(
        mount for mount in container["volumeMounts"] if mount["name"] == "pcf-evidence"
    )
    assert evidence_mount == {
        "name": "pcf-evidence",
        "mountPath": "/mnt/archive/pcf/pcf_effect_evidence",
        "readOnly": True,
    }
    firebase_secrets = {
        volume["secret"]["secretName"]
        for volume in pod["volumes"]
        if volume["name"].startswith("firebase-")
    }
    assert firebase_secrets == {
        "routespark-pcf-effect-writer-firebase-admin",
        "routespark-pcf-effect-verifier-firebase-admin",
    }
    rendered = (CONFIG).read_text(encoding="utf-8")
    assert (
        "PCF_FIRESTORE_WRITER_SERVICE_ACCOUNT: "
        "routespark-pcf-projection-fb@routespark-1f47d.iam.gserviceaccount.com"
    ) in rendered
    assert (
        "PCF_VERIFIER_SERVICE_ACCOUNT: "
        "routespark-pcf-verifier@routespark-1f47d.iam.gserviceaccount.com"
    ) in rendered
    assert (
        "PCF_OBJECT_PROJECTOR_SERVICE_ACCOUNT: "
        "routespark-pcf-object-projector@routespark-1f47d.iam.gserviceaccount.com"
    ) in rendered
    assert (
        "PCF_OBJECT_CLEANER_SERVICE_ACCOUNT: "
        "routespark-pcf-object-cleaner@routespark-1f47d.iam.gserviceaccount.com"
    ) in rendered
    assert "firebase-cleaner.json" not in (BASE / "deployment.yaml").read_text(
        encoding="utf-8"
    )


def test_checked_in_controls_are_valid_and_grant_no_effects():
    configmaps = {item["metadata"]["name"]: item for item in documents(CONFIG)}
    config = configmaps["pcf-effects-config"]["data"]
    assert config["PCF_EFFECT_WORKER_ENABLED"] == "false"
    assert config["PCF_POSTGRES_POOL_MAX"] == "3"

    runtime = parse_runtime_control(
        json.loads(configmaps["pcf-runtime-controls"]["data"]["control.json"])
    )
    assert not runtime.allows_effects
    effect = parse_effect_control(
        json.loads(configmaps["pcf-effect-control"]["data"]["effect-control.json"])
    )
    assert not effect.effects_enabled
    assert effect.fences == ()


def test_effect_network_policy_has_only_postgres_and_https_egress():
    policies = documents(BASE / "networkpolicy.yaml")
    ports = {
        (port["protocol"], port["port"])
        for policy in policies
        for rule in policy["spec"]["egress"]
        for port in rule["ports"]
    }
    assert ports == {
        ("UDP", 53),
        ("TCP", 53),
        ("TCP", 443),
        ("TCP", 5432),
    }
    dns = next(
        policy
        for policy in policies
        if policy["metadata"]["name"] == "allow-pcf-effects-egress-dns"
    )
    target = dns["spec"]["egress"][0]["to"][0]
    assert target == {
        "namespaceSelector": {
            "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
        },
        "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
    }
