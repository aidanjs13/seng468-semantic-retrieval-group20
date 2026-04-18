import os
import psycopg
from celery import Celery
from app import insert_to_vectordb

# this is initializing the celery system
cel = Celery("tasks", broker=os.getenv("RABBIT_URL"))

# database URL for document status update
db_url = os.getenv("DATABASE_URL")

# helper for modifying document status
# this is only invoked once right now, but could be useful in the future
# also didn't want to hardcode status update
def doc_status_update(document_id, status):
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE documents
                SET status = %s
                WHERE document_id = %s
                """,
                (status, document_id)
            )
        conn.commit()


# Notes about this:
# - Not sure on ideal max retry amount, leaving default for now
# - Maybe add a status for failure?
# https://docs.celeryq.dev/en/main/userguide/tasks.html
@cel.task(autoretry_for=(Exception,), retry_backoff=True)
def process_doc(user_id, document_id, stored_path):
    insert_to_vectordb(user_id, document_id, stored_path)
    doc_status_update(document_id, "ready")
