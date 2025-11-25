"""Utilities for tagging catalog ingest runs as unique exercises.

Each invocation of the ingest pipeline creates a new ExerciseTracker which
records HTTP interactions, entity creation attempts, and downstream
verification results. The resulting manifest can be replayed later to audit a
specific run without colliding with prior executions.
"""
from __future__ import annotations

import copy
import json
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class ExerciseTracker:
    exercise: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    entity_attempts: List[Dict[str, Any]] = field(default_factory=list)
    verification_results: List[Dict[str, Any]] = field(default_factory=list)
    verification_summary: Dict[str, Any] = field(default_factory=dict)

    def record_entity_attempt(
        self,
        *,
        kind: str,
        endpoint: str,
        request_payload: Optional[Dict[str, Any]],
        response_payload: Optional[Dict[str, Any]],
        status_code: Optional[int],
        success: bool,
        identifiers: Optional[Dict[str, Any]],
        expected: Optional[Dict[str, Any]],
        request_id: Optional[str],
        attempt_type: str = "primary",
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        entry: Dict[str, Any] = {
            "timestamp": round(time.time(), 3),
            "kind": kind,
            "endpoint": endpoint,
            "attemptType": attempt_type,
            "statusCode": status_code,
            "success": bool(success),
        }
        if identifiers:
            entry["identifiers"] = copy.deepcopy(identifiers)
        if request_payload is not None:
            entry["request"] = copy.deepcopy(request_payload)
        if response_payload is not None:
            entry["response"] = copy.deepcopy(response_payload)
        if expected:
            entry["expected"] = copy.deepcopy(expected)
        if request_id:
            entry["requestId"] = request_id
        if notes:
            entry["notes"] = notes
        attach_context(entry)
        self.entity_attempts.append(entry)
        return entry

    def record_verification_result(self, result: Dict[str, Any]) -> None:
        self.verification_results.append(copy.deepcopy(result))

    def set_verification_summary(self, summary: Dict[str, Any]) -> None:
        self.verification_summary = copy.deepcopy(summary)

    def update_metadata(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            if value is not None:
                self.metadata[key] = value

    def finish(self) -> None:
        if "completedAt" not in self.exercise:
            self.exercise["completedAt"] = _utc_now()

    def build_manifest(self) -> Dict[str, Any]:
        return {
            "exercise": copy.deepcopy(self.exercise),
            "metadata": copy.deepcopy(self.metadata),
            "entityAttempts": copy.deepcopy(self.entity_attempts),
            "verification": {
                "summary": copy.deepcopy(self.verification_summary),
                "results": copy.deepcopy(self.verification_results),
            },
        }

    def write_manifest(self, artifacts_dir: Path) -> Path:
        manifest = self.build_manifest()
        path = artifacts_dir / "exercise_manifest.json"
        path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
        return path


_current_tracker: Optional[ExerciseTracker] = None
_http_context_stack: List[Dict[str, Any]] = []


def _utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def start_exercise(workbook_path: Path, *, mode: str) -> ExerciseTracker:
    global _current_tracker
    exercise = {
        "id": f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:12]}",
        "startedAt": _utc_now(),
        "mode": mode,
        "workbookName": workbook_path.name,
        "workbookPath": str(workbook_path.resolve()),
    }
    tracker = ExerciseTracker(exercise=exercise)
    _current_tracker = tracker
    return tracker


def get_current_tracker() -> Optional[ExerciseTracker]:
    return _current_tracker


def record_entity_attempt(**kwargs: Any) -> Optional[Dict[str, Any]]:
    tracker = get_current_tracker()
    if tracker is None:
        return None
    return tracker.record_entity_attempt(**kwargs)


def record_verification_result(result: Dict[str, Any]) -> None:
    tracker = get_current_tracker()
    if tracker is None:
        return
    tracker.record_verification_result(result)


def set_verification_summary(summary: Dict[str, Any]) -> None:
    tracker = get_current_tracker()
    if tracker is None:
        return
    tracker.set_verification_summary(summary)


def update_metadata(**kwargs: Any) -> None:
    tracker = get_current_tracker()
    if tracker is None:
        return
    tracker.update_metadata(**kwargs)


def finish_and_write(artifacts_dir: Path) -> Optional[Path]:
    tracker = get_current_tracker()
    if tracker is None:
        return None
    tracker.finish()
    return tracker.write_manifest(artifacts_dir)


def current_http_context() -> Dict[str, Any]:
    tracker = get_current_tracker()
    ctx: Dict[str, Any] = {}
    if tracker is not None:
        ctx["exerciseId"] = tracker.exercise.get("id")
    for item in _http_context_stack:
        ctx.update(item)
    return ctx


def attach_context(entry: Dict[str, Any], *, extra: Optional[Dict[str, Any]] = None, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ctx = dict(context or current_http_context())
    if extra:
        ctx.update(extra)
    if ctx:
        entry.setdefault("context", {}).update(ctx)
    return entry


@contextmanager
def http_context(**fields: Any):
    _http_context_stack.append({k: v for k, v in fields.items() if v is not None})
    try:
        yield
    finally:
        _http_context_stack.pop()
