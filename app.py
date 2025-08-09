import os
from flask import Flask, render_template, request, redirect, url_for, jsonify
from dotenv import load_dotenv

# Ensure .env is loaded BEFORE importing modules that may initialize clients
load_dotenv()

from app_services.extractor import extract_product_signals
from app_services.searcher import build_queries, search_candidates
from app_services.scorer import score_candidates


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.post("/analyze")
    def analyze():
        url = request.form.get("url", "").strip()
        if not url:
            return redirect(url_for("index"))

        # Step 0: Extract signals from seed URL
        seed_signals = extract_product_signals(url)

        # Step 1: Build queries and fetch candidates via SerpAPI
        queries = build_queries(seed_signals)
        candidate_urls = search_candidates(queries, original_url=url)

        # Step 1b: Score candidates and pick top 3-5
        top_candidates = score_candidates(seed_signals, candidate_urls)

        # Prepare view model
        results = {
            "seed": {
                "url": url,
                "signals": seed_signals,
            },
            "competitors": [
                {
                    "domain": item["domain"],
                    "url": item["url"],
                    "similarity": round(item["similarity"], 3),
                    "signals": item.get("signals", {}),
                }
                for item in top_candidates
            ],
        }

        return render_template("results.html", results=results)

    @app.get("/health")
    def health():
        return jsonify({"ok": True})

    return app

if __name__ == '__main__':
    app.run(debug=True)
