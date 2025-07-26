# ❄️ Snowflake ⬌ AWS S3 Integration  
**Secure Data Loading & Unloading using SnowSQL + Storage Integration**

This project demonstrates how to securely integrate **Snowflake** with **AWS S3** using **IAM-based Storage Integration** and **External Stages**, allowing you to load and unload data using **SnowSQL** — without hardcoding AWS credentials.

---

## 📌 Features

- 🔐 Secure integration using AWS IAM Role + Snowflake Storage Integration
- 📂 External Stage to connect Snowflake with S3
- 📥 Load raw data from S3 into Snowflake using SnowSQL
- 📤 Unload curated data from Snowflake to S3
- ✅ Follows best practices (no access keys stored)

---

---

## ⚙️ Step-by-Step Setup

### 🔐 1. Create IAM Role in AWS

Create an IAM role in AWS that Snowflake can assume.

**Trust Policy:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "snowflake.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
---
````
## 🧊 2. Create Storage Integration in Snowflake
CREATE STORAGE INTEGRATION my_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-account-id>:role/<your-snowflake-role>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-name/');

  ## 🌐 3. Create External Stage
  CREATE OR REPLACE STAGE my_s3_stage
  STORAGE_INTEGRATION = my_s3_integration
  URL = 's3://your-bucket-name/';

  ##🔎 Validation & Testing
✅ Use LIST @my_s3_stage to verify S3 connection.

✅ Use DESC STAGE my_s3_stage to inspect stage config.

✅ Check AWS S3 to confirm objects were moved.




