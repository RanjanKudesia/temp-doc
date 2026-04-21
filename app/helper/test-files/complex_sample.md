# Complex Markdown Test Document

**Project:** temp-doc  
**Owner:** Platform Team  
**Date:** 2026-04-21

[Jump to Metrics](#2-metrics) | [Jump to Tasks](#5-task-list)

---

## 1. Purpose

This markdown sample is intentionally dense and varied to test parsing, editing, and transformation pipelines.
It includes headings, emphasis, links, tables, lists, checklists, quotes, images, code blocks, inline HTML,
footnotes, and mixed content.

> Reliability and clarity are features, not side effects.

### 1.1 Inline styles

- **Bold text**, *italic text*, and ***bold+italic***
- ~~Strikethrough~~ and `inline code`
- Escapes: \*literal asterisk\*, \_literal underscore\_

---

## 2. Metrics

### 2.1 KPI Table

| Metric | Current | Target | Status |
|---|---:|---:|---|
| Availability | 99.94% | 99.90% | On Track |
| P95 API Latency | 182 ms | 200 ms | On Track |
| Error Rate | 0.21% | 0.30% | On Track |
| SLA Breaches | 4 | 5 | On Track |

### 2.2 Nested lists

1. Platform updates
   - Scaling policies tuned
   - Query optimization complete
   - Cache warm-up job improved
2. Application updates
   - Input validator hardened
   - Retry strategy revised
     - Exponential backoff
     - Jitter enabled
     - Max retries = 4

---

## 3. Technical Snippets

### 3.1 JSON block

```json
{
  "service": "temp-doc",
  "build": "2026.04.21.3",
  "features": ["extract", "edit", "generate"],
  "flags": {
    "strict_validation": true,
    "async_pipeline": false,
    "enable_cache": true
  }
}
```

### 3.2 Python block

```python
def summarize_metrics(rows: list[dict]) -> dict:
    best = min(rows, key=lambda r: r["error_rate"])
    return {
        "best_region": best["region"],
        "error_rate": best["error_rate"],
        "evaluated": len(rows),
    }
```

### 3.3 Bash block

```bash
source ../myvenv/bin/activate
python -m unittest tests/test_edit_route_dispatch.py -v
```

---

## 4. Rich Content

### 4.1 Links and image

- Docs: [Pylint C0415 guidance](https://pylint.readthedocs.io/en/latest/user_guide/messages/convention/import-outside-toplevel.html)
- API home: [Example API](https://example.com/api)

![Placeholder image](https://via.placeholder.com/400x120 "Sample visual")

### 4.2 Inline HTML

<div>
  <strong>Inline HTML block:</strong> Useful to test markdown engines that preserve embedded HTML.
</div>

### 4.3 Definition-like section

Term: Patch instruction  
Meaning: A structured operation containing op/path/value metadata.

Term: Normalization  
Meaning: Post-edit index and structure alignment.

---

## 5. Task List

- [x] Build sample payloads
- [x] Validate markdown rendering
- [ ] Add malformed edge-case sample
- [ ] Add multilingual sample

---

## 6. Footnotes and references

This document includes a footnote reference.[^pipeline]

[^pipeline]: Pipeline order usually follows parse -> validate -> transform -> normalize.

---

### End Note

Small text, mixed punctuation, and numeric formats:
- Amounts: USD 100.00, EUR 89.50, INR 7499.00
- Dates: 2026-04-21, 21/04/2026, Apr 21 2026
- Versions: v1.0.0, v1.2.3-rc1
