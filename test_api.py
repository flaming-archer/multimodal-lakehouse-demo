import urllib.request
import json

BASE = "http://localhost:8888"

def get(path):
    req = urllib.request.Request(f"{BASE}{path}")
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())

def post(path, body):
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(f"{BASE}{path}", data=data,
        headers={"Content-Type": "application/json"}, method="POST")
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())

tests = []

# 1. Health
r = get("/api/health")
assert r["status"] == "ok", f"Health failed: {r}"
tests.append("Health check: PASS")

# 2. Overview
r = get("/api/overview")
assert r["platform"] == "多模态湖仓处理平台"
assert r["gravitino"]["catalogs"] == 3
assert r["storage"]["lance_datasets"] == 2
tests.append("Overview: PASS")

# 3. Demo transcripts
r = get("/api/voice/demo-transcripts")
assert r["count"] == 3, f"Expected 3 transcripts, got {r['count']}"
tests.append(f"Demo transcripts: PASS ({r['count']} transcripts)")

# 4. Voice analysis
r = post("/api/voice/analyze", {
    "transcript": "用户：我要转网，因为套餐太贵了而且信号也差。",
    "call_id": "test_001"
})
assert r["status"] == "success"
assert r["intent"] in ["转网咨询", "携号转网"], f"Unexpected intent: {r['intent']}"
assert len(r["reasons"]) >= 2, f"Expected >=2 reasons, got {r['reasons']}"
assert r["risk_level"] in ["高", "中", "低"]
assert r["sentiment"] in ["负面", "中性偏负", "中性"]
tests.append(f"Voice analysis: PASS (intent={r['intent']}, risk={r['risk_level']}, reasons={r['reasons']})")

# 5. Batch analyze all
r = post("/api/voice/demo-analyze-all", {})
assert r["status"] == "success"
assert r["count"] == 3
tests.append(f"Batch analyze: PASS ({r['count']} results)")

# 6. Storage
r = get("/api/storage/datasets")
assert len(r["datasets"]) == 2
tests.append(f"Storage datasets: PASS ({len(r['datasets'])} Lance datasets)")

r = get("/api/storage/tables")
assert len(r["tables"]) == 2
tests.append(f"Storage tables: PASS ({len(r['tables'])} Iceberg tables)")

# 7. Gravitino
r = get("/api/gravitino/catalogs")
assert len(r["catalogs"]) == 3
tests.append(f"Gravitino catalogs: PASS ({len(r['catalogs'])} catalogs)")

# 8. Schema listing
r = get("/api/gravitino/schemas/lance_catalog")
assert len(r["schemas"]) == 3
tests.append(f"Gravitino schemas: PASS ({len(r['schemas'])} schemas in lance_catalog)")

# 9. Frontend
req = urllib.request.Request(f"{BASE}/")
resp = urllib.request.urlopen(req)
html = resp.read().decode("utf-8")
assert "多模态湖仓处理平台" in html
assert "WebSocket" in html
tests.append("Frontend: PASS (HTML loaded)")

# Summary
print("=" * 50)
print("API TEST RESULTS")
print("=" * 50)
for t in tests:
    print(f"  {t}")
print("=" * 50)
print(f"ALL {len(tests)} TESTS PASSED")
