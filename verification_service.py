"""Post-ingest verification and auto-healing service.

This module inspects the recorded entity attempts for a given ingestion
exercise, re-fetches the persisted data from Revelator, compares it against the
expected payload, and optionally retries POST operations to heal missing data.
"""
from __future__ import annotations

import copy
from typing import Any, Dict, Iterable, List, Optional, Tuple

from exercise_tracker import (
    http_context,
    record_entity_attempt,
    record_verification_result,
    set_verification_summary,
)

# Fields to compare per entity type. Only keys present in the original request
# are checked so optional blanks do not trigger mismatches.
_FIELDS_TO_VERIFY: Dict[str, Tuple[str, ...]] = {
    "artist": ("name", "isni", "artistExternalIds"),
    "label": ("name",),
    "publisher": ("name", "ipiCae", "countryId"),
    "composer": ("name", "isni", "ipiCae", "countryOfResidenceId"),
    "release": ("name", "version", "releaseDate", "upc", "artistExternalIds", "labelId", "primaryMusicStyleId", "secondaryMusicStyleId"),
    "track": ("name", "version", "languageId", "explicit", "trackType", "trackNumber", "previewStartSeconds", "artistId", "artistExternalIds", "trackProperties"),
}


def extract_expected_fields(kind: str, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    fields = _FIELDS_TO_VERIFY.get(kind)
    if not fields:
        return {}
    expected: Dict[str, Any] = {}
    for field in fields:
        if field not in payload:
            continue
        value = payload[field]
        if value is None:
            continue
        expected[field] = copy.deepcopy(value)
    return expected


class VerificationService:
    def __init__(
        self,
        *,
        session: Any,
        base_url: str,
        token: str,
        headers: Dict[str, str],
        enterprise_id: int,
        http_call,
    ) -> None:
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.headers = headers
        self.enterprise_id = enterprise_id
        self.http = http_call
        self.stats = {"total": 0, "matched": 0, "healed": 0, "failed": 0, "skipped": 0}

    # ====== Public API ======

    def run(self, attempts: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        for attempt in attempts:
            if attempt.get("attemptType") not in (None, "primary"):
                continue
            result = self._verify_attempt(attempt)
            record_verification_result(result)
        status = "completed"
        if self.stats["total"] == 0:
            status = "skipped"
        summary = {"status": status, "counts": copy.deepcopy(self.stats)}
        set_verification_summary(summary)
        return summary

    # ====== Dispatch ======

    def _verify_attempt(self, attempt: Dict[str, Any]) -> Dict[str, Any]:
        kind = attempt.get("kind") or "unknown"
        self.stats["total"] += 1
        if kind == "artist":
            return self._verify_artist(attempt)
        if kind == "release":
            return self._verify_release(attempt)
        if kind == "track":
            return self._verify_track(attempt)
        if kind in ("label", "publisher", "composer"):
            # Best-effort verification for ancillary masters.
            return self._verify_generic_master(attempt)
        self.stats["skipped"] += 1
        return self._result(attempt, status="skipped", reason="unsupported_kind")

    # ====== Entity specific checks ======

    def _verify_artist(self, attempt: Dict[str, Any]) -> Dict[str, Any]:
        expected = attempt.get("expected") or {}
        if not expected:
            self.stats["skipped"] += 1
            return self._result(attempt, status="skipped", reason="no_expected_fields")
        artist_id = self._dig(attempt, "identifiers", "artistId")
        name = self._dig(attempt, "identifiers", "name")
        data = self._fetch_artist(artist_id, name)
        if not isinstance(data, dict):
            self.stats["failed"] += 1
            return self._result(attempt, status="failed", reason="fetch_failed")
        mismatches = self._compare_fields("artist", expected, data)
        if not mismatches:
            self.stats["matched"] += 1
            return self._result(attempt, status="matched")
        healed = self._auto_heal(attempt)
        if healed:
            data = self._fetch_artist(artist_id, name)
            mismatches = self._compare_fields("artist", expected, data)
        if mismatches:
            self.stats["failed"] += 1
            return self._result(attempt, status="failed", mismatches=mismatches)
        self.stats["healed"] += 1
        return self._result(attempt, status="healed")

    def _verify_release(self, attempt: Dict[str, Any]) -> Dict[str, Any]:
        expected = attempt.get("expected") or {}
        trackers = [f for f in ("upc", "name") if self._dig(attempt, "identifiers", f)]
        if not expected:
            self.stats["skipped"] += 1
            return self._result(attempt, status="skipped", reason="no_expected_fields")
        release_id = self._dig(attempt, "identifiers", "releaseId")
        upc = self._dig(attempt, "identifiers", "upc")
        name = self._dig(attempt, "identifiers", "name")
        data = self._fetch_release(release_id, upc, name)
        if not isinstance(data, dict):
            self.stats["failed"] += 1
            return self._result(attempt, status="failed", reason="fetch_failed")
        mismatches = self._compare_fields("release", expected, data)
        if not mismatches:
            self.stats["matched"] += 1
            return self._result(attempt, status="matched")
        healed = self._auto_heal(attempt)
        if healed:
            data = self._fetch_release(release_id, upc, name)
            mismatches = self._compare_fields("release", expected, data)
        if mismatches:
            self.stats["failed"] += 1
            return self._result(attempt, status="failed", mismatches=mismatches)
        self.stats["healed"] += 1
        return self._result(attempt, status="healed")

    def _verify_track(self, attempt: Dict[str, Any]) -> Dict[str, Any]:
        expected = attempt.get("expected") or {}
        if not expected:
            self.stats["skipped"] += 1
            return self._result(attempt, status="skipped", reason="no_expected_fields")
        track_id = self._dig(attempt, "identifiers", "trackId")
        isrc = self._dig(attempt, "identifiers", "isrc")
        name = self._dig(attempt, "identifiers", "name")
        data = self._fetch_track(track_id, isrc, name)
        if not isinstance(data, dict):
            self.stats["failed"] += 1
            return self._result(attempt, status="failed", reason="fetch_failed")
        mismatches = self._compare_fields("track", expected, data)
        if not mismatches:
            self.stats["matched"] += 1
            return self._result(attempt, status="matched")
        healed = self._auto_heal(attempt)
        if healed:
            data = self._fetch_track(track_id, isrc, name)
            mismatches = self._compare_fields("track", expected, data)
        if mismatches:
            self.stats["failed"] += 1
            return self._result(attempt, status="failed", mismatches=mismatches)
        self.stats["healed"] += 1
        return self._result(attempt, status="healed")

    def _verify_generic_master(self, attempt: Dict[str, Any]) -> Dict[str, Any]:
        expected = attempt.get("expected") or {}
        if not expected:
            self.stats["skipped"] += 1
            return self._result(attempt, status="skipped", reason="no_expected_fields")
        entity_id = self._dig(attempt, "identifiers", f"{attempt.get('kind')}Id")
        name = self._dig(attempt, "identifiers", "name")
        data = self._fetch_master(attempt.get("kind"), entity_id, name)
        if not isinstance(data, dict):
            self.stats["failed"] += 1
            return self._result(attempt, status="failed", reason="fetch_failed")
        mismatches = self._compare_fields(attempt.get("kind"), expected, data)
        if not mismatches:
            self.stats["matched"] += 1
            return self._result(attempt, status="matched")
        healed = self._auto_heal(attempt)
        if healed:
            data = self._fetch_master(attempt.get("kind"), entity_id, name)
            mismatches = self._compare_fields(attempt.get("kind"), expected, data)
        if mismatches:
            self.stats["failed"] += 1
            return self._result(attempt, status="failed", mismatches=mismatches)
        self.stats["healed"] += 1
        return self._result(attempt, status="healed")

    # ====== Core helpers ======

    def _auto_heal(self, attempt: Dict[str, Any]) -> bool:
        endpoint = attempt.get("endpoint")
        if not endpoint:
            return False
        payload = attempt.get("request")
        if not isinstance(payload, dict):
            return False
        retry_payload = copy.deepcopy(payload)
        identifiers = attempt.get("identifiers") or {}
        for key, value in identifiers.items():
            if key.endswith("Id") and value is not None and key not in retry_payload:
                retry_payload[key] = value
        expected = extract_expected_fields(attempt.get("kind", ""), retry_payload)
        with http_context(phase="verification_retry", entityKind=attempt.get("kind")):
            resp = self.http(self.session, "POST", endpoint, self.token, json_body=retry_payload, headers=self.headers)
        try:
            response_payload = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else None
        except Exception:
            response_payload = None
        record_entity_attempt(
            kind=attempt.get("kind", "unknown"),
            endpoint=endpoint,
            request_payload=retry_payload,
            response_payload=response_payload if isinstance(response_payload, dict) else None,
            status_code=getattr(resp, "status_code", None),
            success=bool(getattr(resp, "ok", False)),
            identifiers=identifiers,
            expected=expected,
            request_id=getattr(resp, "_catalog_request_id", None),
            attempt_type="verification_retry",
            notes="auto_heal_retry",
        )
        return bool(getattr(resp, "ok", False))

    def _compare_fields(self, kind: str, expected: Dict[str, Any], actual: Dict[str, Any]) -> List[Dict[str, Any]]:
        mismatches: List[Dict[str, Any]] = []
        for field, expect in expected.items():
            actual_value = self._extract_field(kind, field, actual)
            if self._equals(field, expect, actual_value):
                continue
            mismatches.append({"field": field, "expected": expect, "actual": actual_value})
        return mismatches

    def _equals(self, field: str, expected: Any, actual: Any) -> bool:
        if field == "artistExternalIds":
            return self._normalize_external_ids(expected) == self._normalize_external_ids(actual)
        if isinstance(expected, list):
            return expected == (actual or [])
        return expected == actual

    def _normalize_external_ids(self, value: Any) -> List[Tuple[int, str]]:
        result: List[Tuple[int, str]] = []
        if not isinstance(value, list):
            return result
        for item in value:
            if not isinstance(item, dict):
                continue
            ds = item.get("distributorStoreId")
            pid = item.get("profileId")
            try:
                ds_int = int(ds)
            except Exception:
                continue
            pid_str = str(pid).strip()
            if not pid_str:
                continue
            result.append((ds_int, pid_str))
        return sorted(result)

    # ====== Result helpers ======

    def _result(
        self,
        attempt: Dict[str, Any],
        *,
        status: str,
        reason: Optional[str] = None,
        mismatches: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        result = {
            "kind": attempt.get("kind"),
            "status": status,
            "identifiers": copy.deepcopy(attempt.get("identifiers")),
        }
        if reason:
            result["reason"] = reason
        if mismatches:
            result["mismatches"] = mismatches
        return result

    # ====== Fetch helpers ======

    def _fetch_artist(self, artist_id: Any, name: Any) -> Optional[Dict[str, Any]]:
        if artist_id:
            for path in (
                f"/artists/{artist_id}",
                f"/api/enterprises/{self.enterprise_id}/artists/{artist_id}",
                f"/api/artists/{artist_id}",
            ):
                data = self._get_json(path)
                if isinstance(data, dict):
                    return data
        if name:
            results = self._search("/api/enterprises/{enterprise_id}/artists", str(name))
            if results:
                return results[0]
        return None

    def _fetch_release(self, release_id: Any, upc: Any, name: Any) -> Optional[Dict[str, Any]]:
        if release_id:
            for path in (
                f"/content/release/{release_id}",
                f"/content/releases/{release_id}",
            ):
                data = self._get_json(path)
                if isinstance(data, dict):
                    return data
        candidates = self._search("/content/release/all", name or upc)
        norm_target = self._normalize_code(upc)
        for item in candidates:
            if norm_target and self._normalize_code(item.get("upc") or item.get("UPC")) == norm_target:
                return item
        return candidates[0] if candidates else None

    def _fetch_track(self, track_id: Any, isrc: Any, name: Any) -> Optional[Dict[str, Any]]:
        if track_id:
            for path in (
                f"/content/track/{track_id}",
                f"/content/tracks/{track_id}",
            ):
                data = self._get_json(path)
                if isinstance(data, dict):
                    return data
        candidates = self._search("/content/track/all", name or isrc)
        norm_target = self._normalize_code(isrc)
        for item in candidates:
            if norm_target and self._track_has_isrc(item, norm_target):
                return item
        return candidates[0] if candidates else None

    def _fetch_master(self, kind: Optional[str], entity_id: Any, name: Any) -> Optional[Dict[str, Any]]:
        if not kind:
            return None
        if entity_id:
            for path in (
                f"/content/{kind}/{entity_id}",
                f"/content/{kind}s/{entity_id}",
            ):
                data = self._get_json(path)
                if isinstance(data, dict):
                    return data
        search_path = f"/content/{kind}/all"
        candidates = self._search(search_path, name)
        if candidates:
            lower = str(name).strip().lower() if name else None
            if lower:
                for item in candidates:
                    nm = str(item.get("name") or "").strip().lower()
                    if nm == lower:
                        return item
            return candidates[0]
        return None

    # ====== HTTP helpers ======

    def _get_json(self, path: str) -> Optional[Any]:
        url = f"{self.base_url}{path}"
        with http_context(phase="verification_fetch", path=path):
            resp = self.http(self.session, "GET", url, self.token, headers=self.headers)
        if not getattr(resp, "ok", False):
            return None
        try:
            return resp.json()
        except Exception:
            return None

    def _search(self, path: str, search_text: Optional[str]) -> List[Dict[str, Any]]:
        if not search_text:
            return []
        url = f"{self.base_url}{path}"
        params = {"searchText": str(search_text), "pageNumber": 1, "pageSize": 25}
        with http_context(phase="verification_fetch", path=path, searchText=search_text):
            resp = self.http(self.session, "GET", url, self.token, params=params, headers=self.headers)
        if not getattr(resp, "ok", False):
            return []
        try:
            data = resp.json()
        except Exception:
            return []
        items = []
        if isinstance(data, dict):
            items = data.get("items") or data.get("results") or []
        elif isinstance(data, list):
            items = data
        return [item for item in items if isinstance(item, dict)]

    # ====== Field extraction helpers ======

    def _extract_field(self, kind: str, field: str, data: Dict[str, Any]) -> Any:
        if field in data:
            return data[field]
        if kind == "release" and field == "artistExternalIds":
            return data.get("artistExternalIds") or data.get("artistsExternalIds")
        if kind == "track" and field == "artistExternalIds":
            return data.get("artistExternalIds") or data.get("artistsExternalIds")
        return None

    # ====== Utility ======

    def _dig(self, obj: Dict[str, Any], *keys: str) -> Any:
        cur: Any = obj
        for key in keys:
            if not isinstance(cur, dict) or key not in cur:
                return None
            cur = cur[key]
        return cur

    def _normalize_code(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        return "".join(ch for ch in s if ch.isalnum())

    def _track_has_isrc(self, item: Dict[str, Any], norm_target: Optional[str]) -> bool:
        if not norm_target:
            return False
        candidates = [item.get("isrc"), item.get("ISRC"), item.get("trackIsrc"), item.get("recordingIsrc")]
        for candidate in candidates:
            if self._normalize_code(candidate) == norm_target:
                return True
        versions = item.get("trackRecordingVersions") or item.get("recordingVersions") or []
        for entry in versions:
            if not isinstance(entry, dict):
                continue
            for candidate in (entry.get("isrc"), entry.get("ISRC")):
                if self._normalize_code(candidate) == norm_target:
                    return True
        return False


def run_verification(
    *,
    session: Any,
    base_url: str,
    token: str,
    headers: Dict[str, str],
    enterprise_id: int,
    http_call,
    attempts: Iterable[Dict[str, Any]],
) -> Dict[str, Any]:
    service = VerificationService(
        session=session,
        base_url=base_url,
        token=token,
        headers=headers,
        enterprise_id=enterprise_id,
        http_call=http_call,
    )
    return service.run(attempts)
