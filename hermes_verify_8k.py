"""
Ad-hoc verification: sec_8k_instruction.json structural check.
Hermes-generated one-shot script — not a project test suite.
Run: python hermes_verify_8k.py
"""
import json, sys, os

ROOT   = os.path.dirname(os.path.abspath(__file__))
target = os.path.join(ROOT, "config", "sec_8k_instruction.json")
ref    = os.path.join(ROOT, "config", "stage2_instruction.json")

errors, passes = [], []

def ok(msg):   passes.append(msg);  print(f"PASS  {msg}")
def fail(msg): errors.append(msg);  print(f"FAIL  {msg}")

# 1 ── JSON validity ──────────────────────────────────────────────────────────
try:
    with open(target, encoding="utf-8") as f:
        doc = json.load(f)
    ok(f"JSON valid  ({os.path.getsize(target):,} bytes)")
except (json.JSONDecodeError, FileNotFoundError) as e:
    fail(f"JSON parse/file error: {e}"); sys.exit(1)

with open(ref, encoding="utf-8") as f:
    ref_doc = json.load(f)

# 2 ── Top-level keys ─────────────────────────────────────────────────────────
for k in ("_description","role","context","item_code_scoring_guide",
          "scoring_bands_8k","calibration","special_rules","tasks",
          "output_schema","rules"):
    (ok if k in doc else fail)(f"top-level key '{k}'")

# 3 ── _description exact ─────────────────────────────────────────────────────
expected = ("SEC 8-K Current Report scorer. Event-driven scoring using Item codes. "
            "Output schema identical to stage2_instruction.json.")
(ok if doc.get("_description") == expected else fail)("_description exact match")

# 4 ── item_code_scoring_guide — 15 codes ─────────────────────────────────────
guide = doc.get("item_code_scoring_guide", {})
for code in ("1.01","1.02","1.03","2.01","2.02","2.05","2.06",
             "3.01","4.01","4.02","5.01","5.02","7.01","8.01","9.01"):
    (ok if code in guide else fail)(
        f"item_code_scoring_guide['{code}'] = {guide.get(code,{}).get('label','MISSING')}")

# 5 ── scoring_bands_8k — 17 bands + valid ranges ─────────────────────────────
bands = doc.get("scoring_bands_8k", {})
for lbl in ("EPOCH_DEFINING","TRANSFORMATIVE","EXCEPTIONAL","VERY_STRONG","STRONG",
            "CLEARLY_POSITIVE","POSITIVE","SLIGHTLY_POSITIVE","WEAK_POSITIVE","NEUTRAL"):
    (ok if lbl in [b["label"] for b in bands.get("POSITIVE",[])] else fail)(
        f"POSITIVE band '{lbl}'")
for lbl in ("SLIGHTLY_NEGATIVE","MILDLY_NEGATIVE","NEGATIVE","VERY_NEGATIVE",
            "EXTREMELY_NEGATIVE","EXISTENTIAL_THREAT","CATASTROPHIC"):
    (ok if lbl in [b["label"] for b in bands.get("NEGATIVE",[])] else fail)(
        f"NEGATIVE band '{lbl}'")
for polarity in ("POSITIVE","NEGATIVE"):
    for b in bands.get(polarity, []):
        lo, hi = b.get("range",[None,None])
        valid = (lo is not None and hi is not None and -1.0 <= min(lo,hi) and max(lo,hi) <= 1.0)
        (ok if valid else fail)(f"band '{b.get('label')}' range {b.get('range')} in [-1,1]")

# 6 ── calibration F1-F10 ─────────────────────────────────────────────────────
cal = doc.get("calibration", {})
for i in range(1, 11):
    m = [k for k in cal if k.startswith(f"F{i}_")]
    (ok if m else fail)(f"calibration F{i}" + (f" → '{m[0]}'" if m else " → MISSING"))

# 7 ── special_rules — 5 rules + content checks ───────────────────────────────
sr = doc.get("special_rules", {})
for rule in ("item_2_02_earnings_release","item_5_02_ceo_cfo_change",
             "item_4_01_auditor_change","item_2_06_impairment","item_3_01_delisting"):
    (ok if rule in sr else fail)(f"special_rules['{rule}']")

checks = [
    ("item_2_02_earnings_release", ["net balance","earnings"],       "earnings/net-balance logic"),
    ("item_3_01_delisting",        ["very_negative","floor","-0.9"], "VERY_NEGATIVE floor"),
    ("item_4_01_auditor_change",   ["mildly_negative","benign"],     "MILDLY_NEGATIVE/benign"),
    ("item_2_06_impairment",       ["market cap","market_cap"],      "market-cap scaling"),
    ("item_5_02_ceo_cfo_change",   ["sudden","planned","retire"],    "sudden vs planned logic"),
]
for key, phrases, label in checks:
    if key in sr:
        text = json.dumps(sr[key]).lower()
        (ok if any(p in text for p in phrases) else fail)(f"{key} content: {label}")

# 8 ── tasks ≥10 entries ──────────────────────────────────────────────────────
tasks = doc.get("tasks", [])
(ok if isinstance(tasks, list) and len(tasks) >= 10 else fail)(
    f"tasks is list with {len(tasks) if isinstance(tasks,list) else '?'} entries (need ≥10)")

# 9 ── output_schema mirrors stage2 ──────────────────────────────────────────
schema, ref_schema = doc.get("output_schema",{}), ref_doc.get("output_schema",{})
for k in ref_schema:
    (ok if k in schema else fail)(f"output_schema key '{k}'")

# 10 ── extracted_facts sub-keys ──────────────────────────────────────────────
for k in ref_schema.get("extracted_facts", {}):
    (ok if k in schema.get("extracted_facts",{}) else fail)(f"extracted_facts['{k}']")

# 11 ── score_calibration sub-keys ────────────────────────────────────────────
for k in ref_schema.get("score_calibration", {}):
    (ok if k in schema.get("score_calibration",{}) else fail)(f"score_calibration['{k}']")

# 12 ── key_events sub-keys ───────────────────────────────────────────────────
for k in ref_schema.get("key_events", {}):
    (ok if k in schema.get("key_events",{}) else fail)(f"key_events['{k}']")

# 13 ── rules list + key phrases ──────────────────────────────────────────────
rules = doc.get("rules", [])
(ok if isinstance(rules, list) and len(rules) >= 10 else fail)(
    f"rules list with {len(rules) if isinstance(rules,list) else '?'} entries (need ≥10)")
rt = " ".join(rules).lower() if isinstance(rules, list) else ""
for phrase, label in (
    ("epoch_defining",        "EPOCH_DEFINING gate"),
    ("catastrophic",          "CATASTROPHIC gate"),
    ("regulatory ladder",     "REGULATORY LADDER"),
    ("exceptional_exclusion", "EXCEPTIONAL_EXCLUSION"),
    ("append_only",           "APPEND_ONLY_NARRATIVE"),
    ("delisting",             "DELISTING floor"),
    ("auditor",               "auditor-change rule"),
    ("impairment",            "impairment scaling"),
):
    (ok if phrase in rt else fail)(f"rules contain '{label}'")

# ── Summary ──────────────────────────────────────────────────────────────────
total = len(passes) + len(errors)
print(f"\n{'='*60}")
print("AD-HOC VERIFICATION  (not a project test suite)")
print(f"File   : {target}")
print(f"Checks : {total}   Passed : {len(passes)}   Failed : {len(errors)}")
if errors:
    print("\nFAILURES:")
    for e in errors: print(f"  ✗ {e}")
    print("\nRESULT: FAILED")
    sys.exit(1)
print("\nRESULT: ALL CHECKS PASSED ✓")
sys.exit(0)
