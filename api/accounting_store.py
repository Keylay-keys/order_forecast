"""Tenant-scoped PostgreSQL transaction, originals, immutable attempts and audit.

Every SQL access includes the verified UID and route. The advisory transaction
lock covers all domain documents for that owner/route across API replicas. No
filesystem fallback exists; database failure fails the request.
"""
from contextlib import contextmanager
import hashlib
from uuid import uuid4

from psycopg2.extras import Json

from .dependencies import get_pg_connection, return_pg_connection


class AccountingStore:
    def __init__(self, cursor, uid, route):
        self.cursor, self.scope = cursor, (uid, route)

    def get(self, key):
        self.cursor.execute("SELECT payload FROM accounting_documents WHERE firebase_uid=%s AND route_number=%s AND document_key=%s", (*self.scope, key))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def put(self, key, value):
        self.cursor.execute("""INSERT INTO accounting_documents (firebase_uid,route_number,document_key,payload)
            VALUES (%s,%s,%s,%s) ON CONFLICT (firebase_uid,route_number,document_key)
            DO UPDATE SET payload=EXCLUDED.payload, updated_at=now()""", (*self.scope, key, Json(value)))
        self.cursor.execute("INSERT INTO accounting_history (event_id,firebase_uid,route_number,document_key,payload) VALUES (%s,%s,%s,%s,%s)", (str(uuid4()), *self.scope, key, Json(value)))

    def documents(self, prefix):
        # Prefix is server-defined; callers cannot select arbitrary namespaces.
        self.cursor.execute("SELECT payload FROM accounting_documents WHERE firebase_uid=%s AND route_number=%s AND starts_with(document_key,%s) ORDER BY document_key", (*self.scope, prefix))
        return [row[0] for row in self.cursor.fetchall()]

    def has_original(self, kind, digest):
        self.cursor.execute("SELECT 1 FROM accounting_originals WHERE firebase_uid=%s AND route_number=%s AND kind=%s AND sha256=%s", (*self.scope, kind, digest))
        return self.cursor.fetchone() is not None

    def queue_payment_advice_cleanup(self, document_number):
        # Reuse the durable email queue. The worker removes only matched advice
        # attachments; MIME parsing and byte rewriting stay off API requests.
        self.cursor.execute("""UPDATE accounting_email_jobs j SET status='queued',attempts=0,
            lease_token=NULL,lease_expires_at=NULL,updated_at=now()
            FROM accounting_email_originals o
            WHERE j.message_id=o.message_id AND o.firebase_uid=%s AND o.route_number=%s
            AND j.status IN ('ready','duplicate','needs_attention')
            AND j.result->'paymentAdviceDocuments' ? %s
            AND NOT (coalesce(j.result->'paymentAdviceRemovedDocuments','[]'::jsonb) ? %s)""",
            (*self.scope, document_number, document_number))

    def add_original(self, kind, upload, file, content):
        self.cursor.execute("""INSERT INTO accounting_originals
            (firebase_uid,route_number,kind,upload_id,file_id,sha256,media_type,original_name,content)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (*self.scope, kind, upload, file["id"], file["sha256"], file.get("mediaType", "application/pdf"), file["originalName"], content))

    def original(self, kind, upload, file):
        self.cursor.execute("""SELECT sha256,media_type,original_name,content FROM accounting_originals
            WHERE firebase_uid=%s AND route_number=%s AND kind=%s AND upload_id=%s AND file_id=%s""", (*self.scope, kind, upload, file))
        row = self.cursor.fetchone()
        if row is None:
            return None
        digest, media_type, name, content = row
        content = bytes(content)
        if hashlib.sha256(content).hexdigest() != digest:
            raise RuntimeError("Original integrity mismatch")
        return dict(sha256=digest, mediaType=media_type, originalName=name, content=content)

    def add_attempt(self, kind, upload, file, attempt_id, payload):
        self.cursor.execute("""INSERT INTO accounting_attempts
            (firebase_uid,route_number,kind,upload_id,file_id,attempt_id,payload) VALUES (%s,%s,%s,%s,%s,%s,%s)""", (*self.scope, kind, upload, file, attempt_id, Json(payload)))

    def attempt(self, kind, upload, file, attempt_id):
        self.cursor.execute("""SELECT payload FROM accounting_attempts
            WHERE firebase_uid=%s AND route_number=%s AND kind=%s AND upload_id=%s AND file_id=%s AND attempt_id=%s""", (*self.scope, kind, upload, file, attempt_id))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def queue_receipt(self, upload, retry=False):
        self.cursor.execute("""INSERT INTO accounting_receipt_jobs (job_id,firebase_uid,route_number,upload_id)
            VALUES (%s,%s,%s,%s) ON CONFLICT (firebase_uid,route_number,upload_id) DO NOTHING""", (str(uuid4()), *self.scope, upload))
        if retry:
            self.cursor.execute("""UPDATE accounting_receipt_jobs SET status='queued',attempts=0,lease_token=NULL,
                lease_expires_at=NULL,completed_attempt_id=NULL,updated_at=now()
                WHERE firebase_uid=%s AND route_number=%s AND upload_id=%s
                AND (status <> 'running' OR lease_expires_at < now())""", (*self.scope, upload))

    def receipt_jobs(self):
        self.cursor.execute("""SELECT upload_id,status,attempts,lease_expires_at<now() FROM accounting_receipt_jobs
            WHERE firebase_uid=%s AND route_number=%s""", self.scope)
        return {str(upload): "needs_attention" if status == "running" and attempts >= 3 and expired else status
                for upload, status, attempts, expired in self.cursor.fetchall()}

    def queue_receipt_agent(self, upload, ocr_attempt):
        self.cursor.execute("""INSERT INTO accounting_receipt_agent_jobs
            (job_id,firebase_uid,route_number,upload_id,ocr_attempt_id)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (firebase_uid,route_number,upload_id,ocr_attempt_id) DO NOTHING""",
            (str(uuid4()), *self.scope, upload, ocr_attempt))

    def receipt_agent_reviews(self):
        self.cursor.execute("""SELECT j.upload_id,j.ocr_attempt_id,j.status,j.attempts,
            j.lease_expires_at<now(),a.payload
            FROM accounting_receipt_agent_jobs j
            LEFT JOIN accounting_attempts a ON a.firebase_uid=j.firebase_uid
                AND a.route_number=j.route_number AND a.kind='receipt'
                AND a.upload_id=j.upload_id AND a.file_id=j.upload_id
                AND a.attempt_id=j.completed_review_id
            WHERE j.firebase_uid=%s AND j.route_number=%s""", self.scope)
        result = {}
        for upload, ocr_attempt, status, attempts, expired, review in self.cursor.fetchall():
            if status == "running" and attempts >= 3 and expired:
                status = "blocked"
            result[str(upload), str(ocr_attempt)] = {"status": status, "review": review}
        return result


@contextmanager
def accounting_transaction(uid, route, *, acquire=get_pg_connection, release=return_pg_connection):
    conn = None
    try:
        conn = acquire()
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '5s'")
            cur.execute("SET LOCAL statement_timeout = '15s'")
            # A hash collision only serializes unrelated scopes; it cannot grant access.
            scope = int.from_bytes(hashlib.sha256((uid + "\0" + route).encode()).digest()[:8], "big", signed=True)
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (scope,))
            yield AccountingStore(cur, uid, route)
        conn.commit()
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            release(conn)
