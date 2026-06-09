# API Documentation

This document describes the APIs used by the frontend (`/archivault-ui`) to interact with the backend infrastructure in `/archival_materials_ingest/web-demo`.

## Authentication Endpoints

These endpoints are handled by the `auth_lambda` lambda function.

### `POST /auth/signup`
Creates a new user account.
* **Lambda Function:** `auth_lambda/auth.py`
* **Request Body:**
  ```json
  {
    "email": "user@example.com",
    "password": "securepassword123"
  }
  ```
* **Response:** Returns an authentication token and account details.

### `POST /auth/login`
Authenticates an existing user and returns a session token.
* **Lambda Function:** `auth_lambda/auth.py`
* **Request Body:**
  ```json
  {
    "email": "user@example.com",
    "password": "securepassword123"
  }
  ```
* **Response:** Returns an authentication token and account details.

### `POST /auth/logout`
Invalidates the user's current session.
* **Lambda Function:** `auth_lambda/auth.py`
* **Headers:** `Authorization: Bearer <token>`
* **Response:** Acknowledges logout completion.

### `GET /auth/me`
Retrieves the authenticated user's details, including remaining credits.
* **Lambda Function:** `auth_lambda/auth.py`
* **Headers:** `Authorization: Bearer <token>`
* **Response:**
  ```json
  {
    "userId": "user@example.com",
    "creditsRemaining": 25
  }
  ```

---

## Job Management Endpoints

### `POST /presign`
Requests presigned Amazon S3 URLs for uploading source files, OR copies files directly from an external S3 bucket if `source_bucket` is specified.
* **Lambda Function:** `presign_lambda/presign.py`
* **Headers:** `Authorization: Bearer <token>`
* **Request Body (Standard Upload Flow):**
  ```json
  {
    "job_title": "My Archival Job",
    "filenames": ["document1.pdf", "image1.jpg"],
    "steps": ["ocr", "translation"],
    "country": "US",
    "state": "CA",
    "description": "Historical documents",
    "metadata": {
      "language": "english"
    }
  }
  ```
* **Request Body (External S3 Bucket Flow):**
  ```json
  {
    "job_title": "My Archival Job",
    "source_bucket": "my-external-s3-bucket",
    "filenames": ["folder/document1.pdf", "folder/image1.jpg"],
    "steps": ["ocr", "translation"],
    "country": "US",
    "state": "CA",
    "description": "Historical documents",
    "metadata": {
      "language": "english"
    }
  }
  ```
* **Response (Standard Upload Flow):** Returns a new `jobId`, a list of `presignedUrls` mapping filenames to S3 upload URLs, and the total `pdf_count`.
* **Response (External S3 Bucket Flow):** Returns a new `jobId`, an empty list of `presignedUrls`, `pdf_count: 0`, and `status: "IMPORTING"`. The S3 copy operation runs in the background.
  ```json
  {
    "jobId": "<uuid>",
    "presignedUrls": [],
    "pdf_count": 0,
    "status": "IMPORTING"
  }
  ```

### `POST /pdf`
Initiates PDF processing (splitting and rendering into images) for uploaded PDF files.
* **Lambda Function:** `pdf_orchestrator/pdf_orchestrator.py`
* **Headers:** `Authorization: Bearer <token>`
* **Request Body:**
  ```json
  {
    "jobId": "<uuid>"
  }
  ```
* **Response:** Details about the number of pages processed or skipped based on PDF size limits.

### `POST /jobs`
Enqueues a job for processing after all files are uploaded and PDFs are derived. Deducts credits based on the image count.
* **Lambda Function:** `enqueue_lambda/enqueue.py`
* **Headers:** `Authorization: Bearer <token>`
* **Request Body:**
  ```json
  {
    "jobId": "<uuid>",
    "steps": ["ocr", "translation"]
  }
  ```
* **Response:** Confirms enqueueing and returns the `jobId`.

### `GET /jobs`
Retrieves the job history for the authenticated user.
* **Lambda Function:** `status_lambda/status_handler.py`
* **Headers:** `Authorization: Bearer <token>`
* **Response:** Returns a list of past job summaries with their respective statuses.

### `GET /jobs/{jobId}`
Polls the status of a specific job and retrieves presigned download URLs for the output artifacts if the job is completed.
* **Lambda Function:** `status_lambda/status_handler.py`
* **Headers:** `Authorization: Bearer <token>`
* **Response:** Returns the current representation of the job dynamically fetched from DynamoDB. When `status` is `"COMPLETED"`, the response will include an `artifacts` dict containing `json`, `markdown`, and conditionally `tables_zip` download links.
