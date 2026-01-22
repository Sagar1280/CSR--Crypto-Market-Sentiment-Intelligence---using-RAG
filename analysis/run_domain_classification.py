import json
from pathlib import Path
from domain_classifier import classify_domain


DATA_DIR = Path("../data/youtube_raw")


def run_domain_classification():
    print("\n=== Phase 6: Domain Classification ===\n")

    files = list(DATA_DIR.glob("*.json"))
    total = len(files)
    classified = 0
    skipped = 0

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        if "domain" in data:
            skipped += 1
            continue

        transcript = data.get("transcript", "")
        if not transcript:
            continue

        print(f"Classifying: {data['title']}")

        result = classify_domain(transcript)

        data["domain"] = result["domain"]
        data["domain_confidence"] = result["confidence"]

        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        classified += 1
        print(f"→ {result['domain']} ({result['confidence']})")

    print("\n=== SUMMARY ===")
    print(f"Total files      : {total}")
    print(f"Classified       : {classified}")
    print(f"Skipped (existing): {skipped}")
    print("================\n")


if __name__ == "__main__":
    run_domain_classification()
