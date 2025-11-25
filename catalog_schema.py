"""Canonical catalog schema for Revelator ingestion.

This module defines the neutral data structures that all ingestion
workflows must adhere to before mapping into Revelator API payloads.
The intent is to provide a single source of truth schema so every
stage (parsing, validation, API mapping, reporting) speaks the same
language.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


@dataclass
class Contributor:
    """Represents a person or entity credited on a release or track."""

    name: str
    roleName: Optional[str] = None
    contributorType: Optional[str] = None
    musicianContributorType: Optional[str] = None
    rawCreditLine: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Contributor":
        return cls(
            name=data.get("name", ""),
            roleName=data.get("roleName"),
            contributorType=data.get("contributorType"),
            musicianContributorType=data.get("musicianContributorType"),
            rawCreditLine=data.get("rawCreditLine"),
            extra=dict(data.get("extra", {})),
        )


@dataclass
class Track:
    """Normalized track representation within the canonical catalog."""

    vendorTrackId: Optional[str]
    isrc: Optional[str]
    numberInRelease: Optional[int]
    title: Optional[str]
    versionTitle: Optional[str] = None
    artistName: Optional[str] = None
    lengthSeconds: Optional[float] = None
    audioLanguageCode: Optional[str] = None
    metadataLanguageCode: Optional[str] = None
    genreDesc: Optional[str] = None
    parentalAdvisory: Optional[str] = None
    originalReleaseDate: Optional[str] = None
    contributors: List[Contributor] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["contributors"] = [c.to_dict() for c in self.contributors]
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Track":
        contributors = [Contributor.from_dict(c) for c in data.get("contributors", [])]
        extra = dict(data.get("extra", {}))
        return cls(
            vendorTrackId=data.get("vendorTrackId"),
            isrc=data.get("isrc"),
            numberInRelease=data.get("numberInRelease"),
            title=data.get("title"),
            versionTitle=data.get("versionTitle"),
            artistName=data.get("artistName"),
            lengthSeconds=data.get("lengthSeconds"),
            audioLanguageCode=data.get("audioLanguageCode"),
            metadataLanguageCode=data.get("metadataLanguageCode"),
            genreDesc=data.get("genreDesc"),
            parentalAdvisory=data.get("parentalAdvisory"),
            originalReleaseDate=data.get("originalReleaseDate"),
            contributors=contributors,
            extra=extra,
        )


@dataclass
class Release:
    """Normalized release representation used across the ingest system."""

    vendorReleaseId: str
    upc: Optional[str]
    grid: Optional[str]
    title: Optional[str]
    versionTitle: Optional[str]
    artistName: Optional[str]
    originalReleaseDate: Optional[str]
    status: Optional[str]
    genreDesc: Optional[str]
    metadataLanguageCode: Optional[str]
    parentalAdvisory: Optional[str]
    pLine: Optional[str]
    cLine: Optional[str]
    totalTracks: Optional[int]
    tracks: List[Track] = field(default_factory=list)
    contributors: List[Contributor] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["tracks"] = [t.to_dict() for t in self.tracks]
        data["contributors"] = [c.to_dict() for c in self.contributors]
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Release":
        tracks = [Track.from_dict(t) for t in data.get("tracks", [])]
        contributors = [Contributor.from_dict(c) for c in data.get("contributors", [])]
        extra = dict(data.get("extra", {}))
        return cls(
            vendorReleaseId=data.get("vendorReleaseId", ""),
            upc=data.get("upc"),
            grid=data.get("grid"),
            title=data.get("title"),
            versionTitle=data.get("versionTitle"),
            artistName=data.get("artistName"),
            originalReleaseDate=data.get("originalReleaseDate"),
            status=data.get("status"),
            genreDesc=data.get("genreDesc"),
            metadataLanguageCode=data.get("metadataLanguageCode"),
            parentalAdvisory=data.get("parentalAdvisory"),
            pLine=data.get("pLine"),
            cLine=data.get("cLine"),
            totalTracks=data.get("totalTracks"),
            tracks=tracks,
            contributors=contributors,
            extra=extra,
        )


@dataclass
class Catalog:
    """Top-level canonical catalog container."""

    releases: List[Release] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {"releases": [release.to_dict() for release in self.releases]}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Catalog":
        releases = [Release.from_dict(r) for r in data.get("releases", [])]
        return cls(releases=releases)

    def __len__(self) -> int:  # pragma: no cover - trivial helper
        return len(self.releases)