# anvil-file-transfer

Transfer controlled-access Anvil files.

Usage:
```bash
$ python transfer.py --help
usage: transfer.py [-h] --env {sandbox,prod} --google-service-account-credentials-base64 GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_BASE64 --portal-key PORTAL_KEY --portal-secret-key PORTAL_SECRET_KEY [--delete-source-files]

options:
  -h, --help            show this help message and exit
  --env {sandbox,prod}  Specify the environment: prod or sandbox.
  --google-service-account-credentials-base64 GOOGLE_SERVICE_ACCOUNT_CREDENTIALS_BASE64
                        Google service account credentials linked to Terra workspace, base64 encoded
  --portal-key PORTAL_KEY
                        Portal key associated with Anvil Transfer service account user
  --portal-secret-key PORTAL_SECRET_KEY
                        Portal secret key associated with Anvil Transfer service account user
  --delete-source-files
                        Delete source files after copying
```