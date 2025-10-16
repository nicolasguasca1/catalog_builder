# Catalog column limits

The ingestion parser only reads the leading columns that carry meaningful data for each sheet. Columns at or beyond the listed cutoff are ignored to avoid wasting time on template notes and diagnostic SQL.

All indices below are **1-based**, matching Excel's column numbering. To adjust the behavior, update `SHEET_COLUMN_LIMITS` in `ingest_parser.py`.

| Sheet name (exact)                | Last column read | Columns ignored  |
| --------------------------------- | ---------------: | ---------------- |
| `1) Artists list`                 |                9 | `10` and onwards |
| `2) Labels list`                  |                2 | `3` and onwards  |
| `3) Release_Label`                |               16 | `17` and onwards |
| `4) Release_Artist(s)`            |                8 | `9` and onwards  |
| `5) Release_Track`                |               16 | `17` and onwards |
| `6) Track_Artist(s)`              |                6 | `7` and onwards  |
| `7) Comp ContributorPublisher li` |                4 | `5` and onwards  |
| `8) Track_Composition(s)`         |               10 | `11` and onwards |
| `9) Audio_Properties`             |               12 | `13` and onwards |

> ℹ️ The sheet titled "Comp Contributor/Publisher lists" in some documentation corresponds to the Excel tab named `7) Comp ContributorPublisher li` in the bundled template.
