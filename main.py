import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from PIL import Image
from docx import Document
from PyPDF2 import PdfReader

import functions_framework
from cloudevents.http.event import CloudEvent
from google.api_core import exceptions as gcp_exceptions
from google.cloud import storage
from google.cloud import speech_v2 as speech

# Logger config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# config
REGION = "us-west1"
OUTPUT_BUCKET = "discovery-processed"
LOCAL_LOG_DIR = "/tmp"
LOCAL_LOG_PATH = "/tmp/logs.txt"
LOG_FOLDER = "logs"
LOG_FILE_NAME = "logs.txt"
LOG_BUCKET_NAME = "discovery-processed"

# init the clients
PROJECT_ID = os.environ.get("GCP_PROJECT")
storage_client = None
speech_client = None

# This might be vertex ai stuff
# aiplatform.init(project=PROJECT_ID, location=REGION)

def init_clients():
    """
    Initialize clients only once.
    """
    global storage_client, speech_client

    if not storage_client:
            storage_client = storage.Client()
            logger.info("Initialized storage client.")

    if not speech_client:
        speech_client = speech.SpeechClient()
        logger.info("Initialized speech client.")

    if storage_client is None:
        logger.error("Failed to initialize storage client!")

    if speech_client is None:
        logger.error("Failed to initialize speech client!")

@functions_framework.cloud_event
def analyze_discovery_material_ce(ce: CloudEvent):
    """
    Triggered by a GCS Eventarc event (storage.objects.create).
    """
    logger.info(f'Called "analyze_discovery_material_ce" successfully')
    init_clients()

    event_type = ce["type"]

    if event_type == "google.cloud.storage.object.v1.finalized":
        file_deleted = handle_new_file_or_change(ce)

        if file_deleted:
            return
        
        try:
            validate_file(ce)
        except Exception as e:
            logger.exception(f"Error validating file : {e}")
        
        # All these following functions attempt to do something based on the filetype
        # NOTE: It's not neccessary that all of these will be executed
        # So make these functions log when they are called
        speech_to_text(ce)

        logger.info("To be continued! :)")

def transcribe_gcs(gcs_uri: str) -> str:
    """
    Uses Speech-to-Text v2 API to transcribe audio from GCS.
    Returns the full transcript as string.
    Requires a recognizer resource to be created beforehand.
    """
    client = speech.SpeechClient()

    request = speech.RecognizeRequest(
        recognizer=f"projects/{PROJECT_ID}/locations/us-west1/recognizers/my-recognizer",
        config=speech.RecognitionConfig(
            auto_decoding_config={},  # auto detect encoding
            language_codes=["en-US"],
            features=speech.RecognitionFeatures(enable_automatic_punctuation=True),
            model="long",
        ),
        uri=gcs_uri,
    )

    response = client.recognize(request=request)

    transcript = "\n".join(
        [result.alternatives[0].transcript for result in response.results]
    )
    return transcript.strip()


def speech_to_text(ce: CloudEvent):
    """
    Wrapper around v2 API that saves transcript in discovery-processed bucket,
    preserving folder structure.
    """
    event_data = ce.data
    file_name = event_data.get("name")
    bucket_name = event_data.get("bucket")

    ext = get_file_extension(file_name)
    if ext not in (".mp3", ".wav", ".flac", ".m4a", ".ogg"):
        logger.info(f"Skipping speech-to-text: unsupported file type '{ext}'")
        return

    gcs_uri = f"gs://{bucket_name}/{file_name}"
    logger.info(f"🎧 Transcribing {gcs_uri} via Speech-to-Text v2...")

    try:
        transcript = transcribe_gcs(gcs_uri)
        if not transcript:
            transcript = "[No transcription available]"

        # Save transcript to bucket, same folder structure
        processed_blob_name = f"{os.path.splitext(file_name)[0]}.txt"
        dest_blob = storage_client.bucket(OUTPUT_BUCKET).blob(processed_blob_name)
        tmp_path = f"/tmp/{os.path.basename(processed_blob_name)}"
        with open(tmp_path, "w") as f:
            f.write(transcript)

        dest_blob.upload_from_filename(tmp_path, content_type="text/plain")
        logger.info(f"📝 Uploaded transcription to gs://{OUTPUT_BUCKET}/{processed_blob_name}")
        gcs_log(f"Transcribed {gcs_uri} → gs://{OUTPUT_BUCKET}/{processed_blob_name}")

    except Exception as e:
        logger.exception(f"❌ Speech-to-text v2 failed for {gcs_uri}: {e}")
        gcs_log(f"Speech-to-text failed for {gcs_uri}: {e}", severity="ERROR")

def get_file_path(file_name):
    """
    Get file path without the actual filename.
    This simply returns the directory structure of the file
    """
    file_name = file_name.split("/")
    file_name = file_name[:-1]
    return "/".join(file_name)
        
def create_folder(bucket_name: str, folder_name: str):
    """
    Create a 'folder' (zero-byte object with trailing slash) in the specified GCS bucket.
    This is useful for logical organization — GCS itself is flat, but the console treats
    objects ending with "/" as folders.
    """
    try:
        folder_name = folder_name.lstrip("/")
        if not folder_name.endswith("/"):
            folder_name += "/"

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(folder_name)

        # Upload empty content to simulate a folder
        blob.upload_from_string("", content_type="application/x-directory")

        logger.info(f"📁 Created folder '{folder_name}' in bucket '{bucket_name}'.")
    except Exception as e:
        logger.exception(f"❌ Failed to create folder '{folder_name}' in bucket '{bucket_name}': {e}")

def handle_new_file_or_change(ce: CloudEvent) -> bool:
    """
    Returns True if the file was deleted, False otherwise
    """
    event_data = ce.data
    file_name = event_data.get("name")
    bucket_name = event_data.get("bucket")

    if "/" not in file_name:
        logger.info(f"❌ File {file_name} uploaded at bucket root — not allowed.")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.delete()
        gcs_log(f"Deleted disallowed file {file_name} (no files allowed at bucket root)")
        return True
    else:
        logger.info(f"✅ File {file_name} is inside a folder. Continuing...")
        return False

        
def validate_file(ce: CloudEvent):
    logger.info(f"Trying to validate file!")

    event_data = ce.data
    file_name = event_data.get("name")
    bucket_name = event_data.get("bucket")

    tmp_path = f"/tmp/{os.path.basename(file_name)}"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    logger.info("WE GET HERE:)")
    blob.download_to_filename(tmp_path)
    logger.info(f"📥 Downloaded {file_name} for validation.")
    
    valid, message = validate_file_helper(tmp_path, file_name)

    if valid:
        logger.info(message)
    else:
        logger.warning(message)
        blob.delete()
        gcs_log(f"Deleted corrupted or invalid file {file_name}: {message}\n", severity="ERROR")

def get_file_extension(file_name: str):
    return (os.path.splitext(file_name)[1] or "").lower()

def validate_file_helper(local_path: str, file_name: str) -> tuple[bool, str]:
    """
    Validate various file types by extension/MIME.
    Returns (is_valid, message)
    """
    ext = get_file_extension(file_name)

    # Audio / video files
    if ext in (".mp4", ".mkv", ".mp3", ".wav", ".m4a"):
        if not os.path.exists(local_path):
            return False, f"File download failed: {local_path} does not exist."
        try:
            result = subprocess.run(
                ["ffprobe", "-v", "error", "-i", local_path],
                capture_output = True,
                text=True,
                timeout=30,
            )
            stderr_content = result.stderr.strip()
            if result.returncode != 0 or stderr_content:
                return False, f"Invalid or corrupted media: {file_name}, {stderr_content or 'None-zero exit code'}"
            else:
                return True, f"{file_name} file passed integrity check."
        except subprocess.CalledProcessError as e:
            return False, f"Invalid or corrupted media: {e.stderr.strip()}"
    # Image files
    elif ext in (".jpg", ".jpeg", ".png"):
        try:
            with Image.open(local_path) as img:
                img.verify()
            return True, f"Image file is valid."
        except Exception as e:
            return False, f"Corrupted image file: {e}"
    # Docx
    elif ext == ".docx":
        try:
            _ = Document(local_path)
            return True, f"DOCX file opened successfully."
        except Exception as e:
            return False, f"Corrupted DOCX file: {e}"
    # Pdf
    elif ext == ".pdf":
        try:
            reader = PdfReader(local_path)
            _ = len(reader.pages)
            return True, "PDF file opened successfully."
        except Exception as e:
            return False, f"Corrupted or unreadable PDF: {e}"
    # Unknown / unsupported
    else:
        return False, f"Unsupported file type for {file_name}; skipping validation."

def gcs_log(message: str, severity: str = "INFO", max_retries: int = 5):
    """
    Safely append a log entry to logs.txt in GCS with optimistic concurrency control.
    Uses if_generation_match to avoid race conditions.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log_entry = f"[{timestamp}] [{severity}]: {message}\n"
    bucket = storage_client.bucket(LOG_BUCKET_NAME)
    blob = bucket.blob(f"{LOG_FOLDER}/{LOG_FILE_NAME}")
    current_generation = 0
    upload_kwargs = {}
    
    for attempt in range(1, max_retries + 1):
        try:
            # Downloading existing logs + record generation
            if blob.exists():
                blob.reload()
                current_generation = blob.generation
                blob.download_to_filename(LOCAL_LOG_PATH)
            else:
                current_generation = 0
                open(LOCAL_LOG_PATH, "w").close()

            # Append new line locally
            with open(LOCAL_LOG_PATH, "a") as f:
                f.write(log_entry)

            # Upload with generation precondition
            upload_kwargs = {}
            if current_generation > 0:
                upload_kwargs["if_generation_match"] = current_generation

            blob.upload_from_filename(LOCAL_LOG_PATH, content_type="text/plain", **upload_kwargs)

            logger.info(f"🪵 Successfully appended to logs.txt on attempt {attempt}")
            return
        except gcp_exceptions.PreconditionFailed:
            # Another process wrote before us — retry
            logger.warning(f"⚠️ Generation mismatch on attempt {attempt}, retrying...")
            time.sleep(0.5 * attempt)
            continue
        except Exception as e:
            logger.exception(f"❌ Unexpected error while writing to logs.txt: {e}")
            return
        
        logger.error("❌ Failed to append log after multiple retries due to concurrent writes.")
