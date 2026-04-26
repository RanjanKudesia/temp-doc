import json
import statistics
import pathlib
import re

files = sorted(pathlib.Path("chunking_results").glob("*_chunks.json"))

for f in files:
    data = json.loads(f.read_text())
    raw = data.get("chunks", [])
    if not raw:
        print(f.name, "-> NO CHUNKS")
        continue

    if isinstance(raw[0], dict):
        chunks = [c.get("text", "") for c in raw]
    else:
        chunks = [str(c) for c in raw]

    L = [len(c) for c in chunks]
    total = len(chunks)

    print("=" * 60)
    print("FILE:", f.name)
    print("  chunks       :", total)
    print("  min chars    :", min(L))
    print("  max chars    :", max(L))
    print("  avg chars    :", round(statistics.mean(L)))
    print("  median       :", round(statistics.median(L)))

    empty = [i for i, c in enumerate(chunks) if not c.strip()]
    tiny = [i for i, c in enumerate(chunks) if 0 < len(c) < 100]
    oversized = [i for i, c in enumerate(chunks) if len(c) > 1200]

    md_leak = [i for i, c in enumerate(chunks)
               if re.search(r'\*\*|__|\[.*\]\(.*\)', c)]
    broken = [i for i, c in enumerate(chunks)
              if chunks.index(c) != total - 1 and re.search(r'[a-z,]\s*$', c)]
    lone_p = [i for i, c in enumerate(chunks) if re.fullmatch(r'[\s\W]+', c)]

    seen = {}
    dupes = []
    for i, c in enumerate(chunks):
        key = c.strip()[:80]
        if key in seen:
            dupes.append(i)
        else:
            seen[key] = i

    print("  ISSUES:")
    print("    empty            :", len(empty))
    print("    tiny <100        :", len(tiny),
          "->", [chunks[i][:60] for i in tiny[:3]])
    print("    oversized >1200  :", len(oversized))
    print("    markdown leaking :", len(md_leak),
          "->", [chunks[i][:80] for i in md_leak[:2]])
    print("    broken mid-sent  :", len(broken))
    print("    lone punctuation :", len(lone_p))
    print("    near-dupes       :", len(dupes))

    print("  FIRST:", repr(chunks[0][:300]))
    if total > 1:
        print("  LAST :", repr(chunks[-1][:300]))
    print()
