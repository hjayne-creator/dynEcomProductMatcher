import os
import json
import time
import uuid
import threading
import queue
from typing import Dict, Any

from flask import Flask, render_template, request, redirect, url_for, jsonify, Response, stream_with_context
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

    # --- Simple in-memory job registry for SSE progress ---
    jobs: Dict[str, Dict[str, Any]] = {}
    jobs_lock = threading.Lock()

    def json_dumps(data: Dict[str, Any]) -> str:
        return json.dumps(data, separators=(",", ":"), ensure_ascii=False)

    def _enqueue(job_id: str, payload: Dict[str, Any]) -> None:
        with jobs_lock:
            job = jobs.get(job_id)
            if not job:
                return
            payload.setdefault("ts", time.time())
            try:
                job["queue"].put_nowait(payload)
            except Exception:
                pass

    def _run_job(job_id: str, url: str) -> None:
        try:
            _enqueue(job_id, {"message": "Starting extraction...", "stage": "extract", "status": "running"})
            seed_signals = extract_product_signals(url)
            _enqueue(job_id, {"message": "Extraction complete", "stage": "extract", "status": "ok"})

            _enqueue(job_id, {"message": "Building queries...", "stage": "search", "status": "running"})
            queries = build_queries(seed_signals)
            _enqueue(job_id, {"message": f"Built {len(queries)} queries", "stage": "search", "status": "ok"})

            _enqueue(job_id, {"message": "Searching candidates...", "stage": "search", "status": "running"})
            candidate_urls = search_candidates(queries, original_url=url)
            _enqueue(job_id, {"message": f"Found {len(candidate_urls)} candidate URLs", "stage": "search", "status": "ok"})

            _enqueue(job_id, {"message": "Scoring candidates...", "stage": "score", "status": "running"})
            top_candidates = score_candidates(seed_signals, candidate_urls)
            _enqueue(job_id, {"message": f"Selected {len(top_candidates)} top matches", "stage": "score", "status": "ok"})

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

            with jobs_lock:
                job = jobs.get(job_id)
                if job is not None:
                    job["status"] = "done"
                    job["results"] = results
            _enqueue(job_id, {"message": "done", "status": "done", "stage": "final"})
        except Exception as exc:  # noqa: BLE001
            with jobs_lock:
                job = jobs.get(job_id)
                if job is not None:
                    job["status"] = "error"
                    job["error"] = str(exc)
            _enqueue(job_id, {"message": f"error: {exc}", "status": "error"})

    @app.post("/analyze")
    def analyze():
        url = request.form.get("url", "").strip()
        if not url:
            return redirect(url_for("index"))

        job_id = uuid.uuid4().hex
        q: queue.Queue = queue.Queue(maxsize=1000)
        with jobs_lock:
            jobs[job_id] = {
                "queue": q,
                "status": "running",
                "results": None,
                "error": None,
                "created_at": time.time(),
                "url": url,
            }

        t = threading.Thread(target=_run_job, args=(job_id, url), daemon=True)
        t.start()

        return render_template("progress.html", job_id=job_id, url=url)

    @app.get("/events/<job_id>")
    def sse_events(job_id: str):
        # Stream events for a given job_id
        def _gen():
            last_heartbeat = 0.0
            while True:
                with jobs_lock:
                    job = jobs.get(job_id)
                if job is None:
                    yield "data: {\"status\": \"error\", \"message\": \"unknown job\"}\n\n"
                    break

                try:
                    payload = job["queue"].get(timeout=5)
                    msg = json_dumps(payload)
                    yield f"data: {msg}\n\n"
                    if payload.get("status") in {"done", "error"}:
                        break
                except Exception:
                    now = time.time()
                    if now - last_heartbeat > 15:
                        # heartbeat comment to keep connection alive
                        yield ": heartbeat\n\n"
                        last_heartbeat = now
                        continue
        headers = {
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
        return Response(stream_with_context(_gen()), mimetype="text/event-stream", headers=headers)

    @app.get("/job/<job_id>")
    def job_progress(job_id: str):
        with jobs_lock:
            job = jobs.get(job_id)
        if not job:
            return redirect(url_for("index"))
        return render_template("progress.html", job_id=job_id, url=job.get("url"))

    @app.get("/job/<job_id>/view")
    def job_view(job_id: str):
        with jobs_lock:
            job = jobs.get(job_id)
        if not job:
            return redirect(url_for("index"))
        if job.get("status") != "done" or not job.get("results"):
            return redirect(url_for("job_progress", job_id=job_id))
        return render_template("results.html", results=job["results"])

    @app.get("/job/<job_id>/status")
    def job_status(job_id: str):
        with jobs_lock:
            job = jobs.get(job_id)
        if not job:
            return jsonify({"status": "unknown"})
        return jsonify({
            "status": job.get("status"),
            "has_results": bool(job.get("results")),
            "error": job.get("error"),
        })

    @app.get("/health")
    def health():
        return jsonify({"ok": True})

    return app

app = create_app()

if __name__ == '__main__':
    app.run(debug=True)