"""Promo Email Listener.

Polls IMAP inboxes for promo emails, extracts XLSX/PDF attachments,
parses them, and queues results into PostgreSQL.

Multi-user: Reads promo settings from Firebase `users/{uid}/settings/promoSettings`
and polls each enabled user's inbox.
"""

from __future__ import annotations

import argparse
import email
import imaplib
import os
import tempfile
import time
from dataclasses import dataclass
from email.message import Message
from pathlib import Path
from typing import List, Optional, Dict

from google.cloud import firestore  # type: ignore

try:
    from .promo_parser import parse_promo_attachment
    from .sap_matcher import match_sap
    from .pg_utils import execute
except ImportError:
    from promo_parser import parse_promo_attachment  # type: ignore
    from sap_matcher import match_sap  # type: ignore
    from pg_utils import execute  # type: ignore


POLL_INTERVAL = 43200  # 12 hours (twice per day)
MAX_RETRIES = 3


@dataclass
class UserPromoConfig:
    """User's promo email configuration from Firebase."""
    user_id: str
    route_number: str
    enabled: bool
    imap_server: str
    imap_port: int
    imap_username: str
    imap_password: str
    folder_to_watch: str
    processed_folder: str
    auto_match: bool
    account_mappings: Dict[str, List[str]]


def connect_imap(server: str, username: str, password: str, port: int = 993) -> imaplib.IMAP4_SSL:
    client = imaplib.IMAP4_SSL(server, port)
    client.login(username, password)
    return client


def move_message(client: imaplib.IMAP4_SSL, msg_id: bytes, folder: str):
    try:
        client.copy(msg_id, folder)
        client.store(msg_id, "+FLAGS", "\\Deleted")
        client.expunge()
    except Exception as e:
        print(f"⚠️  Failed to move message {msg_id} to {folder}: {e}")


def process_attachment(temp_dir: Path, part: Message) -> Path | None:
    filename = part.get_filename()
    if not filename:
        return None
    if not (filename.lower().endswith(".xlsx") or filename.lower().endswith(".xls") or filename.lower().endswith(".pdf")):
        return None
    data = part.get_payload(decode=True)
    if not data:
        return None
    target = temp_dir / filename
    with open(target, "wb") as f:
        f.write(data)
    return target


def queue_email(fb: firestore.Client, route_number: str, email_id: str, subject: str, attachment_name: str, status: str, items_imported: int = 0):
    """Write to both PostgreSQL and Firebase."""
    # Write to PostgreSQL
    execute(
        """
        INSERT INTO promo_email_queue (
            email_id, route_number, subject, received_at, attachment_name, status, items_imported, processed_at
        ) VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (email_id) DO UPDATE SET
            status = EXCLUDED.status,
            items_imported = EXCLUDED.items_imported,
            processed_at = EXCLUDED.processed_at,
            attachment_name = EXCLUDED.attachment_name,
            subject = EXCLUDED.subject
        """,
        [email_id, route_number, subject, attachment_name, status, items_imported],
    )
    
    # Also write to Firebase so the app can see it
    try:
        import datetime
        queue_ref = fb.collection('promoRequests').document(route_number).collection('queue').document(email_id)
        queue_ref.set({
            'emailId': email_id,
            'subject': subject,
            'attachmentName': attachment_name,
            'status': status,
            'itemsImported': items_imported,
            'receivedAt': firestore.SERVER_TIMESTAMP,
            'processedAt': firestore.SERVER_TIMESTAMP,
        }, merge=True)
    except Exception as e:
        print(f"⚠️  Failed to sync queue item to Firebase: {e}")


def process_message(fb: firestore.Client, msg_id: bytes, msg: Message, route_number: str, processed_folder: str, failed_folder: str, client: imaplib.IMAP4_SSL):
    subject = msg.get("Subject", "")
    attachment_paths: List[Path] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            attachment = process_attachment(tmp_path, part)
            if attachment:
                attachment_paths.append(attachment)

        if not attachment_paths:
            queue_email(fb, route_number, msg_id.decode(), subject, "", "failed", 0)
            move_message(client, msg_id, failed_folder)
            return

        try:
            total_items = 0
            for attachment in attachment_paths:
                items = parse_promo_attachment(attachment)
                total_items += len(items)
            queue_email(fb, route_number, msg_id.decode(), subject, attachment_paths[0].name, "processed", total_items)
            move_message(client, msg_id, processed_folder)
        except Exception as e:
            print(f"❌ Error processing {msg_id}: {e}")
            queue_email(fb, route_number, msg_id.decode(), subject, attachment_paths[0].name if attachment_paths else "", "failed", 0)
            move_message(client, msg_id, failed_folder)


def parse_user_config(fb: firestore.Client, user_id: str, settings: dict) -> Optional[UserPromoConfig]:
    """Parse a user's promo settings into a UserPromoConfig."""
    if not settings or not settings.get('enabled'):
        return None
    
    # Get user's route number from their profile
    user_doc = fb.collection('users').document(user_id).get()
    user_data = user_doc.to_dict() or {} if user_doc.exists else {}
    route_number = user_data.get('routeNumber') or user_data.get('route_number') or ''
    
    if not route_number:
        # Try to get from userSettings
        user_settings_ref = fb.collection('users').document(user_id).collection('userSettings').document('settings')
        user_settings = user_settings_ref.get()
        if user_settings.exists:
            us_data = user_settings.to_dict() or {}
            route_number = us_data.get('routeNumber') or ''
    
    if not route_number or not settings.get('imapServer') or not settings.get('imapUsername'):
        return None
        
    return UserPromoConfig(
        user_id=user_id,
        route_number=route_number,
        enabled=settings.get('enabled', False),
        imap_server=settings.get('imapServer', ''),
        imap_port=settings.get('imapPort', 993),
        imap_username=settings.get('imapUsername', ''),
        imap_password=settings.get('imapPassword', ''),
        folder_to_watch=settings.get('folderToWatch', 'INBOX'),
        processed_folder=settings.get('processedFolder', 'Processed'),
        auto_match=settings.get('autoMatch', True),
        account_mappings=settings.get('accountMappings', {}),
    )


def load_user_configs(fb: firestore.Client) -> List[UserPromoConfig]:
    """Load all users with promo settings enabled from Firebase (one-time load)."""
    configs: List[UserPromoConfig] = []
    
    # Query all users
    users_ref = fb.collection('users')
    users = users_ref.stream()
    
    for user_doc in users:
        user_id = user_doc.id
        
        # Check for promo settings
        settings_ref = fb.collection('users').document(user_id).collection('settings').document('promoSettings')
        settings_doc = settings_ref.get()
        
        if not settings_doc.exists:
            continue
            
        settings = settings_doc.to_dict()
        config = parse_user_config(fb, user_id, settings)
        if config:
            configs.append(config)
    
    return configs


def poll_user_inbox(fb: firestore.Client, config: UserPromoConfig):
    """Poll a single user's promo inbox."""
    try:
        print(f"📬 Checking inbox for user {config.user_id} (route {config.route_number})")
        client = connect_imap(
            config.imap_server,
            config.imap_username,
            config.imap_password,
            config.imap_port
        )
        client.select(config.folder_to_watch)
        typ, data = client.search(None, "ALL")
        
        if typ != "OK":
            print(f"⚠️  IMAP search failed for {config.user_id}")
            return
        
        msg_ids = data[0].split() if data[0] else []
        if msg_ids:
            print(f"   Found {len(msg_ids)} message(s)")
        
        for num in msg_ids:
            typ, msg_data = client.fetch(num, "(RFC822)")
            if typ != "OK":
                continue
            msg = email.message_from_bytes(msg_data[0][1])
            process_message(
                fb,
                num,
                msg,
                config.route_number,
                config.processed_folder,
                "Failed",  # Default failed folder
                client
            )
        
        client.close()
        client.logout()
        
    except Exception as e:
        print(f"⚠️  IMAP error for user {config.user_id}: {e}")


def poll_inbox(args):
    """Legacy single-user mode (command-line args)."""
    fb = firestore.Client.from_service_account_json(args.service_account)

    while True:
        try:
            client = connect_imap(args.imap_server, args.imap_username, args.imap_password, args.imap_port)
            client.select(args.folder)
            typ, data = client.search(None, "ALL")
            if typ != "OK":
                print("⚠️  IMAP search failed")
                time.sleep(POLL_INTERVAL)
                continue

            for num in data[0].split():
                typ, msg_data = client.fetch(num, "(RFC822)")
                if typ != "OK":
                    continue
                msg = email.message_from_bytes(msg_data[0][1])
                process_message(fb, num, msg, args.route, args.processed_folder, args.failed_folder, client)

            client.close()
            client.logout()
        except Exception as e:
            print(f"⚠️  IMAP error: {e}")

        time.sleep(POLL_INTERVAL)


class PromoEmailWatcher:
    """
    Watches Firebase for promo settings changes using on_snapshot (no polling).
    Only polls IMAP inboxes for users with enabled promo settings.
    Also handles manual promo uploads from the app.
    """
    
    def __init__(self, service_account_path: str):
        self.service_account_path = service_account_path
        self.fb = firestore.Client.from_service_account_json(service_account_path)
        self.user_configs: Dict[str, UserPromoConfig] = {}  # user_id -> config
        self.watchers: Dict[str, any] = {}  # user_id -> watcher
        self.running = True
        
    def start(self):
        """Start watching for promo settings and polling inboxes."""
        print("🎧 Promo Email Listener started (real-time mode)", flush=True)
        print("   Watching Firebase for promo settings changes...", flush=True)
        print("   Watching for manual promo uploads...", flush=True)
        print(f"   IMAP poll interval: {POLL_INTERVAL // 3600}h (twice daily)", flush=True)
        
        # Initial load of all users
        self._initial_load()
        
        # Watch for new users
        self._watch_for_new_users()
        
        # Watch for manual promo uploads
        self._watch_for_uploads()
        
        # Main loop: poll IMAP inboxes
        try:
            while self.running:
                self._poll_all_inboxes()
                time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\n👋 Stopping promo email listener...")
            self._cleanup()
    
    def _initial_load(self):
        """Load all existing promo configs and set up watchers."""
        configs = load_user_configs(self.fb)
        for config in configs:
            self._add_user(config)
            print(f"   ✓ Loaded config for user {config.user_id} (route {config.route_number})", flush=True)
        
        if not configs:
            print("   No users with promo settings enabled yet", flush=True)
    
    def _watch_for_new_users(self):
        """Watch the users collection for new promo settings."""
        # We use collection group query to watch all promoSettings docs
        # This fires when any user's promo settings change
        col_group = self.fb.collection_group('settings')
        
        def on_settings_snapshot(col_snapshot, changes, read_time):
            for change in changes:
                if change.type.name not in ('ADDED', 'MODIFIED'):
                    continue
                
                doc = change.document
                # Filter for promoSettings documents
                if doc.id != 'promoSettings':
                    continue
                    
                # Extract user_id from path: users/{user_id}/settings/promoSettings
                try:
                    path_parts = doc.reference.path.split('/')
                    user_id = path_parts[1]  # users/{user_id}/...
                except Exception:
                    continue
                
                settings = doc.to_dict() or {}
                
                if settings.get('enabled'):
                    # User enabled promo import
                    config = parse_user_config(self.fb, user_id, settings)
                    if config:
                        if user_id not in self.user_configs:
                            print(f"\n📬 New user enabled promo import: {user_id}")
                        self._add_user(config)
                else:
                    # User disabled promo import
                    if user_id in self.user_configs:
                        print(f"\n📭 User disabled promo import: {user_id}")
                        self._remove_user(user_id)
        
        self.settings_watcher = col_group.on_snapshot(on_settings_snapshot)
    
    def _add_user(self, config: UserPromoConfig):
        """Add a user to the poll list."""
        self.user_configs[config.user_id] = config
    
    def _remove_user(self, user_id: str):
        """Remove a user from the poll list."""
        if user_id in self.user_configs:
            del self.user_configs[user_id]
    
    def _poll_all_inboxes(self):
        """Poll IMAP inboxes for all enabled users."""
        if not self.user_configs:
            return
        
        print(f"\n📧 Polling {len(self.user_configs)} inbox(es)...")
        for config in self.user_configs.values():
            poll_user_inbox(self.fb, config)
    
    def _watch_for_uploads(self):
        """Watch for manual promo uploads from the app.
        
        App creates docs in: promoRequests/{routeNumber}/uploads/{requestId}
        with: { fileName, storageUrl, status: 'pending' }
        """
        # Watch promoRequests collection group for upload subcollections
        col_group = self.fb.collection_group('uploads')
        
        def on_upload_snapshot(col_snapshot, changes, read_time):
            for change in changes:
                if change.type.name != 'ADDED':
                    continue
                
                doc = change.document
                data = doc.to_dict() or {}
                
                # Only process pending uploads
                if data.get('status') != 'pending':
                    continue
                
                # Extract route from path: promoRequests/{route}/uploads/{id}
                try:
                    path_parts = doc.reference.path.split('/')
                    route_number = path_parts[1]
                except Exception:
                    continue
                
                request_id = doc.id
                file_name = data.get('fileName', '')
                storage_url = data.get('storageUrl', '')
                
                print(f"\n📤 Manual promo upload: {file_name} for route {route_number}")
                
                try:
                    self._process_upload(route_number, request_id, file_name, storage_url, doc.reference)
                except Exception as e:
                    print(f"❌ Error processing upload {request_id}: {e}")
                    doc.reference.update({'status': 'failed', 'error': str(e)})
        
        self.upload_watcher = col_group.on_snapshot(on_upload_snapshot)
    
    def _process_upload(self, route_number: str, request_id: str, file_name: str, storage_url: str, doc_ref):
        """Process a manual promo upload."""
        from google.cloud import storage as gcs
        
        # Download file from Storage
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / file_name
            
            # Download from storage URL
            if storage_url.startswith('gs://'):
                # gs://bucket/path format
                parts = storage_url.replace('gs://', '').split('/', 1)
                bucket_name = parts[0]
                blob_path = parts[1] if len(parts) > 1 else ''
                
                # Use the same service account as Firestore
                storage_client = gcs.Client.from_service_account_json(self.service_account_path)
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(blob_path)
                blob.download_to_filename(str(tmp_path))
            else:
                # HTTPS URL - download directly
                import urllib.request
                urllib.request.urlretrieve(storage_url, str(tmp_path))
            
            # Parse the file
            items = parse_promo_attachment(tmp_path)
            
            if not items:
                doc_ref.update({'status': 'failed', 'error': 'No items found in file'})
                return
            
            print(f"   ✓ Parsed {len(items)} promo items from file")
            
            # Extract unique SAP codes
            all_sap_codes = set()
            for item in items:
                sap = item.get('sap_raw', '') or item.get('sap_code', '')
                if sap:
                    all_sap_codes.add(sap)
            
            # Filter to only SAPs in user's Firebase catalog (source of truth)
            # AND validate SAP-description matches
            try:
                # Fetch user's product catalog from Firebase with full names
                catalog_ref = self.fb.collection('masterCatalog').document(route_number).collection('products')
                catalog = {}  # sap -> fullName
                for doc in catalog_ref.stream():
                    data = doc.to_dict()
                    catalog[doc.id] = (data.get('fullName') or data.get('name') or '').lower()
                
                catalog_saps = set(catalog.keys())
                
                # Validate SAP-description matches and filter
                valid_items = []
                mismatched = 0
                for item in items:
                    sap = item.get('sap_raw', '') or item.get('sap_code', '')
                    if not sap or sap not in catalog_saps:
                        continue  # SAP not in catalog
                    
                    # Check if description roughly matches catalog name
                    desc = (item.get('description', '') or '').lower()
                    catalog_name = catalog.get(sap, '')
                    
                    # Extract key words from description for matching
                    desc_words = set(desc.replace(',', ' ').replace('&', ' ').split())
                    catalog_words = set(catalog_name.replace(',', ' ').replace('&', ' ').split())
                    
                    # Remove common words
                    common = {'mission', 'guerrero', 'calidad', 'the', 'and', 'or', 'ct', '8ct', '10ct', '20ct', 'soft', 'taco', 'flour', 'tortilla', 'tortillas'}
                    desc_key = desc_words - common
                    catalog_key = catalog_words - common
                    
                    # Check for overlap - if no overlap, likely misaligned
                    overlap = desc_key & catalog_key
                    if len(overlap) == 0 and len(desc_key) > 0 and len(catalog_key) > 0:
                        # Possible mismatch - check for brand match at least
                        brands = {'mission', 'guerrero', 'calidad'}
                        desc_brand = desc_words & brands
                        catalog_brand = catalog_words & brands
                        if desc_brand != catalog_brand:
                            mismatched += 1
                            print(f"   ⚠️  SAP mismatch: {sap} desc='{item.get('description', '')[:40]}' vs catalog='{catalog_name[:40]}'")
                            continue  # Skip mismatched items
                    
                    valid_items.append(item)
                
                if mismatched > 0:
                    print(f"   ⚠️  Skipped {mismatched} items with SAP-description mismatches")
                
                items = valid_items
                sap_codes = list(set(
                    (item.get('sap_raw', '') or item.get('sap_code', ''))
                    for item in items if (item.get('sap_raw', '') or item.get('sap_code', ''))
                ))
                print(f"   ✓ Filtered to {len(sap_codes)} valid SAPs in user's catalog (out of {len(all_sap_codes)} total)")
                
            except Exception as e:
                print(f"   ⚠️  Could not filter by catalog: {e}")
                sap_codes = list(all_sap_codes)
            
            # Write to promos/{route}/active/{requestId}
            promo_ref = self.fb.collection('promos').document(route_number).collection('active').document(request_id)
            promo_ref.set({
                'promoId': request_id,
                'promoName': file_name,
                'affectedSaps': sap_codes,
                'items': items[:500],  # Limit to prevent huge docs
                'itemCount': len(items),
                'uploadedAt': firestore.SERVER_TIMESTAMP,
                'status': 'active',
            })
            
            # Update request status
            doc_ref.update({
                'status': 'processed',
                'itemsFound': len(items),
                'sapCount': len(sap_codes),
                'processedAt': firestore.SERVER_TIMESTAMP,
            })
            
            print(f"   ✓ Wrote {len(sap_codes)} SAP codes to promos/{route_number}/active")
    
    def _cleanup(self):
        """Clean up watchers."""
        self.running = False
        if hasattr(self, 'settings_watcher'):
            self.settings_watcher.unsubscribe()
        if hasattr(self, 'upload_watcher'):
            self.upload_watcher.unsubscribe()


def poll_all_users(service_account_path: str):
    """Multi-user mode: Watch for promo settings and poll IMAP inboxes."""
    watcher = PromoEmailWatcher(service_account_path)
    watcher.start()


def main():
    parser = argparse.ArgumentParser(description="Promo email listener")
    parser.add_argument("--service-account", required=True, dest="service_account")
    parser.add_argument("--multi-user", action="store_true", 
                        help="Poll all users with promo settings enabled (reads config from Firebase)")
    
    # Legacy single-user args (optional with --multi-user)
    parser.add_argument("--route", required=False, help="Route number (single-user mode)")
    parser.add_argument("--imap-server", required=False, help="IMAP server (single-user mode)")
    parser.add_argument("--imap-port", type=int, default=993)
    parser.add_argument("--imap-username", required=False, help="IMAP username (single-user mode)")
    parser.add_argument("--imap-password", required=False, help="IMAP password (single-user mode)")
    parser.add_argument("--folder", default="INBOX")
    parser.add_argument("--processed-folder", default="Processed")
    parser.add_argument("--failed-folder", default="Failed")
    args = parser.parse_args()

    if args.multi_user:
        # Multi-user mode: read settings from Firebase
        print("🚀 Starting in multi-user mode...")
        poll_all_users(args.service_account)
    else:
        # Legacy single-user mode
        if not all([args.route, args.imap_server, args.imap_username, args.imap_password]):
            parser.error("Single-user mode requires: --route, --imap-server, --imap-username, --imap-password")
        poll_inbox(args)


if __name__ == "__main__":
    main()
