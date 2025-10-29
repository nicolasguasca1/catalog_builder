# API endpoints overview

This document lists every API endpoint the current ingestion script (`ingest_parser.py`) calls, with a concise mapping of each request parameter to its source spreadsheet column (sheet name + column) or derived value. It reflects the script's present behavior (not planned future fields like ISNI/IPI/ISWC that are visible in sheets but not yet wired into payloads).

---

## Lookup (GET) endpoints

1. `/content/label/all` (and fallback `/content/labels/all`)

   - Purpose: Fetch existing labels to reuse instead of creating duplicates.
   - Keys used: `label.name` (lowercased) from response to build a lookup; matched against `2) Labels list` → `Label Name`.

2. `/content/publisher/all` (fallback `/content/publishers/all`)

   - Purpose: Fetch existing publishers.
   - Keys used: `publisher.name` (lowercased) matched against `7) Comp ContributorPublisher li` → `Publisher Name`.

3. `/common/lookup/contributorRoles`

   - Purpose: Resolve role names to `roleId` for contributors/composers (filters groupId == 4).
   - Source mapping: Track & release contributor rows from sheets `4) Release_Artist(s)` and `6) Track_Artist(s)` (`ARTIST ROLE`); composition roles from `8) Track_Composition(s)` (`ROLE`). Normalized text matched to returned role names.

4. `/api/enterprises/{enterpriseId}/artists`

   - Purpose: Attempt to find existing artist by name before creating.
   - Source mapping: Query param `name` from `1) Artists list` → `Artist Name`.

5. `/common/lookup/languages`

   - Purpose: Map language name to `languageId`.
   - Source mapping:
     - Release: `3) Release_Label` → `TITLE LANGUAGE`.
     - Tracks: `5) Release_Track` → `LANGUAGE OF LYRICS` / `LANGUAGE`.
     - Composer locals: `8) Track_Composition(s)` → `TRACK VERSION` (for version) and release language for `languageId`.

6. `/common/lookup/musicstyles`
   - Purpose: Map genre names to `musicStyleId`.
   - Source mapping: `3) Release_Label` → `GENRE 1` → `primaryMusicStyleId`; `GENRE 2` → `secondaryMusicStyleId`.

---

## Media ingestion (POST) endpoints

1. `/media/audio/pullexternal/{ext}` (e.g. `/wav`)

   - Purpose: Pull audio from an external URL and register it (returns `audioId`).
   - Body:
     - `externalUrl`: From `5) Release_Track` resolved via column aliases (`AUDIO FILE URL`, `AUDIO URL`, `AUDIO DOWNLOAD URL`, etc.).
     - `fileName`: Derived from the audio URL (extension forced to match detected type). File type column (`AUDIO TYPE` / `FILE TYPE` / `FORMAT`) informs extension normalization.
   - Result used in: Track payload `trackRecordingVersions[0].audioFiles[0].audioId`.

2. `/media/image/upload`
   - Purpose: Upload artwork assets (release covers via `cover=true`, artist profile images via `cover=false`), returning a `fileId`.
   - Source URLs:
     - Release covers: `3) Release_Label` → `COVER IMAGE URL` downloaded to `source_artworks/` before upload.
     - Artist images: `1) Artists list` → `Artist Image url` saved to `source_artworks/artists/` prior to upload.
   - Result used in: `image.fileId` / `image.filename` for the corresponding release or artist payload.

---

## Creation / upsert (POST) endpoints

1. `/content/label/save`

   - Body: `{ "name": <Label Name> }` from `2) Labels list` → `Label Name`.
   - Reuse logic: Existing labels matched by lowercase name; if found, script records `labelId` and skips creation.

2. `/artists`

   - Body fields:
     - `name`: `1) Artists list` → `Artist Name`.
     - `artistExternalIds`: Built from optional columns on same sheet: `Apple ArtistId`, `Spotify Artist URI` (normalized to ID), `Meta ArtistId`, `SoundCloud ProfileId` with fixed distributorStoreId mapping (Apple=1, Spotify=9, SoundCloud=68, Meta=309).
       - `image`: When `Artist Image url` is provided, the script downloads the asset, uploads it via `/media/image/upload?cover=false`, and injects `{ filename, fileId }` using the returned identifier.
       - `isni`: When the `ISNI` column contains a valid identifier it is normalized (punctuation stripped, check-digit validated) and attached; invalid values are logged to `identifier_warnings` without blocking the run.

3. `/content/publisher/save`

   - Body: `{ "name": <Publisher Name>, "ipiCae": <optional>, "countryId": <optional> }` from the publisher column group (columns 7–9) on `7) Comp ContributorPublisher li`.
     - `name`: `Publisher Name`.
     - `ipiCae`: Pulled only from the publisher IPI/CAE column (duplicate header distinguished positionally). Normalized to 9- or 11-digit numeric string; invalid/conflicting values logged.
     - `countryId`: Resolved via `/common/lookup/countries` from `Publisher Country`; omitted if lookup fails (warning logged).

4. `/content/composer/save`

   - Body: `{ "name": <Composition Contributor>, "isni": <optional>, "ipiCae": <optional>, "countryOfResidenceId": <optional> }` built from the contributor column group (columns 1–4) of `7) Comp ContributorPublisher li`.
     - `name`: `Composition Contributor`.
     - `isni`: Normalized 16-digit value (punctuation removed) when valid; conflicts logged.
     - `ipiCae`: Taken only from contributor IPI/CAE column (distinct from publisher IPI). 9- or 11-digit normalization; invalid values logged.
     - `countryOfResidenceId`: Resolved via `/common/lookup/countries` from `Contributor Country`; omitted if lookup fails (warning logged).

5. `/content/release/save`

   - Body fields (assembled from multiple sheets):
     - `name`: `3) Release_Label` → `RELEASE TITLE`.
     - `version`: `3) Release_Label` → `RELEASE VERSION`.
     - `previouslyReleased`: Boolean set if `ORIGINAL RELEASE DATE` present in `3) Release_Label`.
     - `releaseDate`: Raw value from `ORIGINAL RELEASE DATE`.
     - `upc`: `3) Release_Label` → `UPC / EAN / JAN` (removed if duplicate error encountered).
     - `copyrightP`: Combined from `(P) Copyright Year` + `Holder` (subheader parsing) or neighbor fallback.
     - `copyrightC`: Combined from `(C) Copyright Year` + `Holder`.
     - `languageId`: Resolved from `TITLE LANGUAGE` via lookup (or fallback map).
     - `releaseLocals[0].name`: Same as `RELEASE TITLE`; `languageId` identical to above.
     - `primaryMusicStyleId`: Resolved from `GENRE 1`.
     - `secondaryMusicStyleId`: Resolved from `GENRE 2`.
     - `hasRecordLabel` / `labelName`: From `LABEL`.
     - `image.fileId` / `image.filename`: From cover image upload (if successful); otherwise `imageSourceUrl` (temporary before upload).
     - `artistName`: From `4) Release_Artist(s)` row where `ARTIST ROLE` == `Main Primary Artist`.
     - `artistExternalIds`: Copied from the artist object matched by `artistName` (Apple/Spotify/Meta/SoundCloud IDs).
     - `contributors[]`: Additional release-level contributors from `4) Release_Artist(s)` with `ARTIST ROLE` mapped to `roleId` (excluding primary artist).
     - `labelId`: Injected after label creation/reuse (from lookup result of `/content/label/save`).
     - `tracks[]`: Array of track payload objects (see `/content/track/save`).

6. `/content/track/save`
   - Body fields (each track also embedded inside release creation payload):
     - `name`: `5) Release_Track` → `TRACK TITLE`.
     - `version`: `5) Release_Track` → `TRACK VERSION`.
     - `languageId`: Resolved from `LANGUAGE OF LYRICS` / `LANGUAGE` (same logic as release).
     - `explicit`: Parsed from `EXPLICIT`.
     - `trackType`: Mapped from `TYPE` (original=1, cover=2, public domain=3).
     - `trackNumber`: Parsed from `TRACK` / `TRACK #` / `TRACK NUMBER`.
     - `previewStartSeconds`: From `TRACK PREVIEW` / `PREVIEW START`.
     - `trackRecordingVersions[0].isrc`: `5) Release_Track` → `ISRC/vISRC`.
     - `trackRecordingVersions[0].recordingVersionType`: Same as `trackType` mapping.
     - `trackRecordingVersions[0].audioFiles[0]`: From audio ingestion endpoint result (`audioId`, plus `audioFilename`, `fileFormat`). Only included if upload succeeded.
     - `artistName`: Set using primary track artist where `ARTIST ROLE` == `Main Primary Artist` from `6) Track_Artist(s)`.
     - `artistExternalIds`: Copied from matched artist (same logic as release). If primary artist found only at release level, these may propagate.
     - `contributors[]`: Non-primary track artists from `6) Track_Artist(s)` with role mapping.
     - `composerContentsDTO[]`: Built from `8) Track_Composition(s)` rows grouped by ISRC:
       - `composerName`: `COMPOSITION CONTRIBUTOR`.
       - `roleId`: Mapped from `ROLE` via contributorRoles lookup.
       - `share`: Numeric share from `SHARE%` scaled to percent format (stringified). Validation ensures totals ~100 or ~1.0.
       - `rightsId`: Derived from `PUBLISHING` text using heuristic (published vs self-published).
         - `isni`: Propagated from the row when provided, otherwise filled from the composer master sheet (normalized by `normalize_isni`).
         - `ipiCae`: Same precedence as `isni`, normalized via `normalize_ipi_cae` (9 or 11 digits only).
       - `composersLocals[0].languageId`: Track languageId; `name`: same as composerName; `version`: track `TRACK VERSION` when present.
       - `compositions[]`: Unique ISWC values consolidated from both `5) Release_Track` (`COMPOSITION ISWC`) and `8) Track_Composition(s)` rows. Values are normalized to `T##########`; duplicates are deduped per track while preserving all distinct codes.
     - `trackProperties[]`: Mapped boolean flags from `9) Audio_Properties` columns (`NONE APPLY` defaults if no flags set). Each special property column (e.g. `REMIX or DERIVATIVE`, `SAMPLES or STOCK`) contributes an ID from `TRACK_PROP_MAP`.
     - `trackLocals[0]`: Included when both `name` and `languageId` exist. Mirrors `name`, `languageId`, and optional `version`.

- Newly included: publisher `ipiCae` & `countryId`; composer `isni`, `ipiCae`, `countryOfResidenceId` (present in master payloads and propagated to per-track composer entries).

---

## Internal derived fields (no direct endpoint yet)

- `imageSourceUrl` (release): Retained only when the cover upload fails in live mode (otherwise removed after obtaining `fileId`).
- Column limit enforcement: `SHEET_COLUMN_LIMITS` restricts header scanning per sheet (documented separately in `docs/column_limits.md`).

---

## DistributorStoreId mapping for `artistExternalIds`

| Source column        | distributorStoreId | Profile extraction           |
| -------------------- | ------------------ | ---------------------------- |
| Apple ArtistId       | 1                  | Raw value                    |
| Spotify Artist URI   | 9                  | Parsed ID (URI/URL stripped) |
| SoundCloud ProfileId | 68                 | Raw value                    |
| Meta ArtistId        | 309                | Raw value                    |

---

## Unused (visible) columns as of current implementation

- (Now wired) `7) Comp ContributorPublisher li`: `Contributor Country` and `Publisher Country` (used for composer `countryOfResidenceId` and publisher `countryId`).
- `8) Track_Composition(s)`: `TOTAL%`, `CATALOG TRACK ID` (shares validation uses `SHARE%` only).
- `9) Audio_Properties`: Helper / diagnostic columns beyond the first 12 (ignored due to column limits).

---

## Summary flow

1. Sheet parsing & column limiting.
2. Lookups (labels, publishers, roles, languages, musicstyles, countries, existing artists).
3. Build entity payloads (artists, labels, publishers, composers) from respective sheets.
4. Media ingestion (audio first; cover images later if live run).
5. Assemble releases and tracks, inject contributor/composer/property data.
6. Live mode: upsert labels/artists, create publishers/composers, upload images, then create releases, then tracks.
7. Emit artifacts summarizing all payloads (`artifacts/*.json`).

---

Last updated: 2025-10-29 after wiring dual IPI columns and country lookups.
