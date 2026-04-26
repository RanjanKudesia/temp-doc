import json
import statistics
import pathlib

data = json.loads(pathlib.Path(
    "chunking_results/complex_sample_md_chunks.json").read_text())
chunks = data.get("chunks", [])

lengths = [len(c) for c in chunks]
print("Total chunks   :", len(chunks))
print("Min chars      :", min(lengths))
print("Max chars      :", max(lengths))
print("Avg chars      :", round(statistics.mean(lengths)))
print("Median chars   :", round(statistics.median(lengths)))
print("Empty chunks   :", sum(1 for c in chunks if not c.strip()))
print("Short (<100)   :", sum(1 for c in chunks if len(c) < 100))
print("Good (350-1200):", sum(1 for c in chunks if 350 <= len(c) <= 1200))
print("Oversized>1200 :", sum(1 for c in chunks if len(c) > 1200))
print()

for label, idx in [("first", 0), ("middle", len(chunks)//2), ("last", -1)]:
    print("---", label, "chunk ---")
    print(repr(chunks[idx][:300]))
    print()

heading_chunks = [c for c in chunks if c.split(
    '\n')[0].startswith(('##', '#', 'Section', 'Table'))]
print("Chunks starting with heading:", len(heading_chunks))

total_chars = sum(lengths)
orig_size = pathlib.Path(
    "../app/helper/test-files/complex_sample.md").stat().st_size
print("Total chars in chunks  :", f"{total_chars:,}")
print("Original file size     :", f"{orig_size:,} bytes")
print("Coverage ratio         :", f"{total_chars/orig_size*100:.1f}%")
