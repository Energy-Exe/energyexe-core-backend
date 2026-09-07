# Partner data upload — Varanger Kraft (secure S3 drop zone)

Instructions for uploading SCADA data files to EnergyExe's secure inbound storage.
The sections below the line are written to be pasted into an email to the uploader.
Credentials are **never** in this doc or in email — send them through a separate
secure channel.

**Infra**: bucket `energyexe-partner-inbound` (eu-north-1, AWS Stockholm), defined in
`infra/partner_inbound.tf`. IAM user `varanger-kraft-upload` can Put/Get/List only
under `varanger-kraft/`, cannot delete, TLS enforced, bucket versioned + encrypted
(AES256), no public access.

---

## Upload instructions (email-ready)

Your upload destination is a secure Amazon S3 storage bucket hosted in the AWS
Stockholm region (**Sweden**):

| Field | Value |
|---|---|
| Bucket | `energyexe-partner-inbound` |
| Upload folder | `varanger-kraft/` |
| Region | `eu-north-1` (AWS Stockholm, Sweden) |
| Access Key ID | *(sent separately, via a secure channel)* |
| Secret Access Key | *(sent separately, via a secure channel)* |

The credentials can **only add files** to the `varanger-kraft/` folder — they cannot
delete anything or see any other data. All transfers are TLS-encrypted, and files
are encrypted and versioned at rest in the Stockholm region.

Pick whichever upload method suits you. **Please follow the connection settings
exactly** — in particular the folder/server fields marked in bold, since the
credentials are scoped to just your folder and generic settings will show a
"not authorized" error.

### Option A — WinSCP (Windows, drag & drop)

1. Download WinSCP (free): https://winscp.net
2. New Session → **File protocol: Amazon S3**
3. Host name: `s3.eu-north-1.amazonaws.com`, Port: 443
4. Access key ID / Secret access key: as provided
5. **Important:** click **Advanced… → Environment → Directories** and set
   **Remote directory** to `/energyexe-partner-inbound/varanger-kraft/`
6. Login → you land directly in the `varanger-kraft` folder → drag your files in.
   Large files upload in resumable parts automatically.

### Option B — Cyberduck (macOS / Windows, drag & drop)

1. Download Cyberduck (free): https://cyberduck.io
2. **Open Connection** → choose **Amazon S3** from the dropdown
3. Server: **`energyexe-partner-inbound.s3.eu-north-1.amazonaws.com`** (Port 443)
   — the bucket name must be part of the server address, exactly as above
4. Access Key ID / Secret Access Key: as provided
5. Connect → you land inside the bucket → open the `varanger-kraft` folder → drag
   files in

> **If you see** *"not authorized to perform: s3:ListAllMyBuckets … Connection
> failed"*: the client is connected to the generic S3 endpoint and is trying to
> browse the whole account, which these credentials deliberately can't do. In
> WinSCP, set the Remote directory as in step 5; in Cyberduck, use the full
> server address from step 3.

### Option C — AWS CLI (scriptable, best for many files)

```bash
export AWS_ACCESS_KEY_ID=<provided>
export AWS_SECRET_ACCESS_KEY=<provided>

# one file
aws s3 cp ./file.csv s3://energyexe-partner-inbound/varanger-kraft/ --region eu-north-1

# a whole folder in one go
aws s3 cp ./export-folder/ s3://energyexe-partner-inbound/varanger-kraft/ --recursive --region eu-north-1
```

### File naming

Any format works for us (CSV preferred). If convenient, a name that identifies the
wind farm, turbine, signal, and period helps us process faster, e.g.
`<farm>_<turbine>_<signal>_<from>_<to>.csv`. Also very welcome: a small README or
signal list describing the datasets (units, sampling notes, timezone of the
timestamps — UTC or local).

---

## Internal: operating notes

- **Credentials handover / rotation** (mimic → real partner, or after any suspicion
  of exposure):
  ```bash
  # list + delete the old key
  aws iam list-access-keys --user-name varanger-kraft-upload --profile energyexe
  aws iam delete-access-key --user-name varanger-kraft-upload --access-key-id <old> --profile energyexe
  # create the replacement (out-of-band — never via Terraform)
  aws iam create-access-key --user-name varanger-kraft-upload --profile energyexe
  ```
- **Before Varanger Kraft starts**: clear any test objects with the admin profile
  (`aws s3 rm s3://energyexe-partner-inbound/varanger-kraft/ --recursive --profile energyexe`).
  The uploader key itself cannot delete.
  ⚠️ A recursive delete also removes the zero-byte `varanger-kraft/` **folder
  marker** — without it the "folder" doesn't exist in S3 and WinSCP fails with
  *"File or folder '/energyexe-partner-inbound/varanger-kraft/' does not exist"*.
  Recreate it after any cleanup:
  `aws s3api put-object --bucket energyexe-partner-inbound --key varanger-kraft/ --profile energyexe`
- **Watch arrivals**: `aws s3 ls s3://energyexe-partner-inbound/varanger-kraft/ --profile energyexe`
- **Next step after delivery**: verified files get copied into the lake at
  `s3://energyexe-scada-data/bronze/landing/varanger-kraft/` and wired into the
  scada pipeline (ingestion mapping depends on the actual export format — pending
  the first batch).
- **New partner later**: add a sibling prefix + IAM user pair in
  `infra/partner_inbound.tf` (copy an existing partner's resources, swap the prefix).
