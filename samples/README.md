# Sample CSV Files

These files match the CSV formats expected by `POST /api/secondary/upload`.

Available samples:

- `ground_truth_sample.csv`
- `bps_sample.csv`
- `bmkg_bpbd_sample.csv`
- `pdam_sample.csv`

Notes:

- Upload one file at a time.
- Set `source_type` to match the file you upload:
  - `ground_truth`
  - `bps`
  - `bmkg`
  - `pdam`
- The API reads CSV headers from the file, so keep the column names exactly the same.
