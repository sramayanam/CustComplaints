"""
Roundtrip test: fetch the current definition (which has valid schemas[] from manual upload)
and push it back unchanged via updateDefinition. If this 200s, then updateDefinition CAN
handle schemas[] and our code issue is something else. If it 400s, schemas[] is entirely
unsupported via updateDefinition.
"""
import json
import base64
import requests

from fabricstreams.register_schemas import _headers
from fabricstreams.config import FABRIC_API_BASE, FABRIC_WORKSPACE_ID, FABRIC_SCHEMA_SET_ID

# Step 1: fetch current definition
resp = requests.post(
    f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/items/{FABRIC_SCHEMA_SET_ID}/getDefinition",
    headers=_headers(), timeout=30,
)
resp.raise_for_status()
parts = resp.json()["definition"]["parts"]
print(f"Fetched {len(parts)} parts")
for p in parts:
    print(f"  {p['path']}")

# Step 2: push it back verbatim (no changes at all)
print("\nPushing verbatim back via updateDefinition ...")
push_resp = requests.post(
    f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/items/{FABRIC_SCHEMA_SET_ID}/updateDefinition",
    headers=_headers(),
    json={"definition": {"parts": parts}},
    timeout=30,
)
print(f"Status: {push_resp.status_code}")
if push_resp.status_code != 200:
    print(f"Body: {push_resp.text[:500]}")
else:
    print("SUCCESS — updateDefinition accepts existing schemas[] structure")
    # Now show what came back
    verify = requests.post(
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/items/{FABRIC_SCHEMA_SET_ID}/getDefinition",
        headers=_headers(), timeout=30,
    )
    verify.raise_for_status()
    vparts = verify.json()["definition"]["parts"]
    defn = json.loads(base64.b64decode(next(p for p in vparts if "EventSchema" in p["path"])["payload"]))
    print(f"  schemas count : {len(defn.get('schemas', []))}")
    print(f"  eventTypes    : {[et['id'] for et in defn.get('eventTypes', [])]}")
import json
import base64
import requests

from fabricstreams.register_schemas import _headers
from fabricstreams.config import FABRIC_API_BASE, FABRIC_WORKSPACE_ID, FABRIC_SCHEMA_SET_ID

resp = requests.post(
    f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/items/{FABRIC_SCHEMA_SET_ID}/getDefinition",
    headers=_headers(),
    timeout=30,
)
resp.raise_for_status()

parts = resp.json()["definition"]["parts"]
print("=== Parts ===")
for p in parts:
    print(f"  {p['path']}")

defn_part = next(p for p in parts if p["path"] == "EventSchemaSetDefinition.json")
defn = json.loads(base64.b64decode(defn_part["payload"]))

print(f"\n=== Top-level keys: {list(defn.keys())} ===")
print(f"  eventTypes count : {len(defn.get('eventTypes', []))}")
print(f"  schemas count    : {len(defn.get('schemas', []))}")

print("\n=== eventTypes[] ===")
for et in defn.get("eventTypes", []):
    keys = list(et.keys())
    print(f"  id={et['id']}  keys={keys}")
    for k, v in et.items():
        if k not in ("schema", "envelopeMetadata"):
            print(f"    {k}: {json.dumps(v)}")

print("\n=== schemas[] — full structure ===")
for s in defn.get("schemas", []):
    print(f"\n  --- schema id={s['id']} ---")
    for k, v in s.items():
        if k == "versions":
            for ver in v:
                print(f"    version keys: {list(ver.keys())}")
                for vk, vv in ver.items():
                    if vk == "schema":
                        parsed = json.loads(vv)
                        print(f"      schema.keys      : {list(parsed.keys())}")
                        print(f"      schema.name      : {parsed.get('name')}")
                        print(f"      schema.type      : {parsed.get('type')}")
                        print(f"      schema.namespace : {parsed.get('namespace', '(none)')}")
                        print(f"      schema.doc       : {parsed.get('doc', '(none)')}")
                        print(f"      schema.fields[0] : {json.dumps(parsed['fields'][0]) if parsed.get('fields') else '(none)'}")
                        print(f"      field types      : {[f['type'] for f in parsed.get('fields', [])]}")
                    else:
                        print(f"      {vk}: {json.dumps(vv)}")
        else:
            print(f"    {k}: {json.dumps(v)}")
    defn = {"eventTypes": event_types, "schemas": schemas}
    platform = _get_platform_payload(FABRIC_SCHEMA_SET_ID)
    payload = {
        "definition": {
            "parts": [
                {
                    "path": "EventSchemaSetDefinition.json",
                    "payload": base64.b64encode(
                        json.dumps(defn).encode()
                    ).decode(),
                    "payloadType": "InlineBase64",
                },
                {"path": ".platform", "payload": platform, "payloadType": "InlineBase64"},
            ]
        }
    }
    resp = requests.post(
        f"{FABRIC_API_BASE}/workspaces/{FABRIC_WORKSPACE_ID}/items/{FABRIC_SCHEMA_SET_ID}/updateDefinition",
        headers=_headers(), json=payload, timeout=30,
    )
    status = resp.status_code
    body = resp.text[:300] if status != 200 else "OK"
    print(f"  [{status}] {label} → {body}")
    return status == 200


envelope = {
    "id":          {"type": "string", "required": True},
    "type":        {"type": "string", "required": True, "value": "T1"},
    "source":      {"type": "string", "required": True},
    "specversion": {"type": "string", "required": True},
}

print("=== Test 1: schemaUrl + schemas[] with only simple types (no long/namespace/doc) ===")
avro_simple = json.dumps({"type": "record", "name": "T1", "fields": [{"name": "id", "type": "string"}]})
update(
    [{"id": "T1", "format": "CloudEvents/1.0", "envelopeMetadata": envelope,
      "schemaUrl": "#/schemas/T1", "schemaFormat": "Avro/1.12.0"}],
    [{"id": "T1", "format": "Avro/1.12.0", "versions": [
        {"id": "v1", "format": "Avro/1.12.0", "schema": avro_simple, "ancestor": "v1"}
    ]}],
    "simple string field, with ancestor",
)

print("\n=== Test 2: same but without ancestor ===")
update(
    [{"id": "T1", "format": "CloudEvents/1.0", "envelopeMetadata": envelope,
      "schemaUrl": "#/schemas/T1", "schemaFormat": "Avro/1.12.0"}],
    [{"id": "T1", "format": "Avro/1.12.0", "versions": [
        {"id": "v1", "format": "Avro/1.12.0", "schema": avro_simple}
    ]}],
    "simple string field, no ancestor",
)

print("\n=== Test 3: add long type ===")
avro_long = json.dumps({"type": "record", "name": "T1", "fields": [{"name": "id", "type": "long"}]})
update(
    [{"id": "T1", "format": "CloudEvents/1.0", "envelopeMetadata": envelope,
      "schemaUrl": "#/schemas/T1", "schemaFormat": "Avro/1.12.0"}],
    [{"id": "T1", "format": "Avro/1.12.0", "versions": [
        {"id": "v1", "format": "Avro/1.12.0", "schema": avro_long}
    ]}],
    "long type",
)

print("\n=== Test 4: add namespace ===")
avro_ns = json.dumps({"type": "record", "name": "T1", "namespace": "com.test", "fields": [{"name": "id", "type": "string"}]})
update(
    [{"id": "T1", "format": "CloudEvents/1.0", "envelopeMetadata": envelope,
      "schemaUrl": "#/schemas/T1", "schemaFormat": "Avro/1.12.0"}],
    [{"id": "T1", "format": "Avro/1.12.0", "versions": [
        {"id": "v1", "format": "Avro/1.12.0", "schema": avro_ns}
    ]}],
    "namespace on record",
)

print("\n=== Test 5: inline schema approach (known-good) ===")
update(
    [{"id": "T1", "format": "CloudEvents/1.0", "envelopeMetadata": envelope,
      "schema": avro_simple, "schemaFormat": "Avro/1.12.0"}],
    [],
    "inline schema (baseline)",
)
