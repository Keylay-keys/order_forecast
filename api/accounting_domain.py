"""Accounting command rules shared by the HTTP service and contract tests.

Amounts are integer cents. Callers supply a transaction-scoped document store;
none of these operations opens a local prototype file or trusts browser totals.
"""
from __future__ import annotations

import calendar
import re
import unicodedata
from copy import deepcopy
from datetime import date, datetime, timezone
from uuid import uuid4

from fastapi import HTTPException

CATEGORIES = frozenset("fuel storage vehicle_note vehicle_maintenance vehicle_insurance business_insurance business_loan equipment office_supplies route_supplies professional licenses_tax technology other".split())
PAYMENT_METHODS = frozenset("card bank cash check other".split())
RECEIPT_METHODS = PAYMENT_METHODS - {"card"}
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)
STORE_ID = re.compile(r"^[A-Za-z0-9_-]{1,50}$")
CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")
LIMIT = 100_000_000


def fail(message="Invalid accounting request.", status=400):
    raise HTTPException(status, message)


def fields(value, allowed, required=()):
    if type(value) is not dict or set(value) - set(allowed) or set(required) - set(value):
        fail()
    return value


def text(value, limit, required=False):
    if not isinstance(value, str) or len(value) > limit or CONTROL.search(value) or (required and not value.strip()):
        fail("Check the text fields.")
    return value.strip()


def key(value):
    return " ".join(unicodedata.normalize("NFKC", value).split()).lower()


def amount(value, signed=False):
    if type(value) is not int or abs(value) > LIMIT or (not signed and value <= 0):
        fail("Enter a valid amount in cents.")
    return value


def day(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        fail("Enter a valid date.")
    try:
        date.fromisoformat(value)
    except ValueError:
        fail("Enter a valid date.")
    return value


def identifier(value):
    if not isinstance(value, str) or not UUID.fullmatch(value):
        fail("Invalid record identifier.")
    return value


def now():
    return datetime.now(timezone.utc).isoformat()


def next_month(anchor, after):
    d = date.fromisoformat(day(after))
    candidate = date(d.year, d.month, min(anchor, calendar.monthrange(d.year, d.month)[1]))
    if candidate <= d:
        year, month = (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)
        candidate = date(year, month, min(anchor, calendar.monthrange(year, month)[1]))
    return candidate.isoformat()


def expense_input(value, today):
    fields(value, ("date", "category", "amountCents", "vendor", "note", "splits", "paymentMethod", "billId"), ("date", "category", "amountCents", "vendor", "note"))
    result = deepcopy(value)
    if day(value["date"]) > today or value["category"] not in CATEGORIES:
        fail("Check the payment date and category.")
    amount(value["amountCents"])
    result["vendor"] = text(value["vendor"], 120, True)
    result["note"] = text(value["note"], 500)
    if "paymentMethod" in value and value["paymentMethod"] not in PAYMENT_METHODS:
        fail("Choose a payment method.")
    if "billId" in value:
        identifier(value["billId"])
    if "splits" in value:
        splits = value["splits"]
        if type(splits) is not list or not 2 <= len(splits) <= 10:
            fail("Choose between two and ten expense categories.")
        for split in splits:
            fields(split, ("category", "amountCents"), ("category", "amountCents"))
            if split["category"] not in CATEGORIES:
                fail()
            amount(split["amountCents"])
        if len({s["category"] for s in splits}) != len(splits) or sum(s["amountCents"] for s in splits) != value["amountCents"]:
            fail("Expense parts must be distinct and add up to the payment.")
        if sorted(splits, key=lambda s: (-s["amountCents"], s["category"]))[0]["category"] != value["category"]:
            fail("The main category must match the largest expense part.")
    return result


class Accounting:
    def __init__(self, store, route, today):
        self.store, self.route, self.today = store, route, day(today)

    def load(self, domain):
        defaults = {"expenses": {"items": []}, "bills": {"items": []}, "payees": {"items": []},
                    "money-in": {"settlements": [], "remittances": []}, "cash-accounts": {"payments": [], "storeAmounts": []},
                    "route-stores": {"stores": [], "selectedStoreIds": [], "handheldStoreIds": [], "refreshedAt": None, "canRefresh": True}}
        value = self.store.get(domain)
        if value is None:
            return dict(schemaVersion=1, route=self.route, revision=0, **deepcopy(defaults[domain]))
        if type(value) is not dict or type(value.get("schemaVersion")) is not int or value["schemaVersion"] != 1 or value.get("route") != self.route or type(value.get("revision")) is not int or value["revision"] < 0:
            raise RuntimeError("Invalid accounting document")
        for name, default in defaults[domain].items():
            if name not in value or (isinstance(default, list) and not isinstance(value[name], list)):
                raise RuntimeError("Invalid accounting document")
        return deepcopy(value)

    def save(self, domain, ledger):
        ledger["revision"] += 1
        self.store.put(domain, ledger)
        return ledger

    def revision(self, ledger, body):
        if type(body.get("revision")) is not int or body["revision"] < 0:
            fail("A valid record revision is required.")
        if body["revision"] != ledger["revision"]:
            fail("Records changed in another tab. Reload before saving.", 409)

    @staticmethod
    def find(items, record_id):
        identifier(record_id)
        found = next((item for item in items if item["id"] == record_id), None)
        if found is None:
            fail("Record not found.", 404)
        return found

    def bills(self, method, record_id, body):
        ledger = self.load("bills")
        if method == "GET" and not record_id:
            return ledger
        fields(body, ("revision", "bill", "action", "requestId"), ("revision",))
        self.revision(ledger, body)
        if method == "POST" and not record_id or method == "PATCH" and record_id and body.get("action") == "edit":
            value = fields(body.get("bill"), ("vendor", "category", "amountCents", "dueDate", "frequency", "note"), ("vendor", "category", "amountCents", "dueDate", "frequency", "note"))
            vendor, note = text(value["vendor"], 120, True), text(value["note"], 500)
            if value["category"] not in CATEGORIES or value["frequency"] not in ("once", "monthly"):
                fail("Check the bill category and schedule.")
            amount(value["amountCents"])
            day(value["dueDate"])
            stamp = now()
            if record_id:
                bill = self.find(ledger["items"], record_id)
            else:
                if len(ledger["items"]) >= 10000:
                    fail("Bill record limit reached.", 409)
                bill = {"id": str(uuid4()), "createdAt": stamp, "status": "active"}
                ledger["items"].insert(0, bill)
            bill.update(value, vendor=vendor, note=note, updatedAt=stamp, anchorDay=int(value["dueDate"][-2:]))
        elif method == "PATCH" and record_id and body.get("action") in ("stop", "resume"):
            bill = self.find(ledger["items"], record_id)
            bill.update(status="stopped" if body["action"] == "stop" else "active", updatedAt=now())
        else:
            fail("Unknown bill action.")
        # A bill is an obligation. No expense is written here.
        return self.save("bills", ledger)

    def expenses(self, method, record_id, body):
        ledger = self.load("expenses")
        if method == "GET" and not record_id:
            # Explicit bill identities are part of the same scoped response.
            return dict(ledger, bills=self.load("bills")["items"])
        fields(body, ("revision", "expense", "action", "monthly", "confirmRecurrenceOff"), ("revision",))
        self.revision(ledger, body)
        stamp = now()
        current = self.find(ledger["items"], record_id) if record_id else None
        if method == "PATCH" and body.get("action") in ("archive", "restore") and current:
            if body["action"] == "archive":
                current["archivedAt"] = stamp
            else:
                current.pop("archivedAt", None)
            current["updatedAt"] = stamp
        elif (method == "POST" and not record_id) or (method == "PATCH" and current and not current.get("archivedAt") and body.get("action") == "edit"):
            value = expense_input(body.get("expense"), self.today)
            monthly = body.get("monthly", bool(current and current.get("recurrence")))
            if type(monthly) is not bool:
                fail("Invalid recurrence setting.")
            bill_id = (current or {}).get("billId") or value.get("billId")
            if current and value.get("billId") and value["billId"] != current.get("billId"):
                fail("A payment cannot be moved to another bill.")
            bills = self.load("bills")
            bill = self.find(bills["items"], bill_id) if bill_id else None
            if bill and "monthly" not in body:
                monthly = bill["frequency"] == "monthly" and bill["status"] == "active"
            if current and current.get("recurrence") and not monthly and body.get("confirmRecurrenceOff") is not True:
                fail("Confirm that future monthly reminders should stop.")
            if monthly and bill is None:
                anchor = int(value["date"][-2:])
                bill = dict(id=str(uuid4()), createdAt=stamp, updatedAt=stamp, vendor=value["vendor"], category=value["category"], amountCents=value["amountCents"], note=value["note"], frequency="monthly", anchorDay=anchor, dueDate=next_month(anchor, self.today), status="active")
                bills["items"].insert(0, bill)
                self.save("bills", bills)
            elif bill and current and "monthly" in body and not monthly and bill["frequency"] == "monthly" and bill["status"] == "active":
                # Stopping a recurring payment updates the one bill, so older siblings
                # cannot resurrect it. Historical payments remain unchanged.
                # Only the bill's explicit Resume action can restart a stopped bill.
                # Editing an older recurring row may carry a stale monthly=True.
                bill.update(status="stopped", updatedAt=stamp)
                self.save("bills", bills)
            if current is None:
                if len(ledger["items"]) >= 10000:
                    fail("Expense record limit reached.", 409)
                current = dict(id=str(uuid4()), createdAt=stamp)
                ledger["items"].insert(0, current)
            for name in ("splits", "paymentMethod", "recurrence"):
                current.pop(name, None)
            current.update(value, updatedAt=stamp)
            if bill:
                current["billId"] = bill["id"]
                if monthly and bill["frequency"] == "monthly" and bill["status"] == "active":
                    anchor = int(value["date"][-2:])
                    current["recurrence"] = dict(frequency="monthly", anchorDay=anchor, firstDueDate=next_month(anchor, max(self.today, value["date"])), enabledAt=stamp)
        else:
            fail("Invalid expense change.")
        return dict(self.save("expenses", ledger), bills=self.load("bills")["items"])

    def payees(self, method, record_id, body):
        ledger = self.load("payees")
        if method == "GET" and not record_id:
            return ledger
        fields(body, ("revision", "action", "payee"))
        if method == "PATCH":
            self.revision(ledger, body)
            current = self.find(ledger["items"], record_id)
            if body.get("action") == "remove":
                ledger["items"].remove(current)  # prior document retained in change history
                return self.save("payees", ledger)
            if body.get("action") != "edit":
                fail()
        elif method == "POST" and not record_id:
            current = None
        else:
            fail()
        value = fields(body.get("payee"), ("name", "defaultCategory"), ("name", "defaultCategory"))
        name = " ".join(text(value["name"], 120, True).split())
        if value["defaultCategory"] not in CATEGORIES:
            fail()
        duplicate = next((p for p in ledger["items"] if key(p["name"]) == key(name)), None)
        if method == "PATCH" and duplicate and duplicate["id"] != current["id"]:
            fail("That payee already exists.", 409)
        if method == "POST":
            current = duplicate
        stamp = now()
        if current is None:
            if len(ledger["items"]) >= 2000:
                fail("Payee record limit reached.", 409)
            current = dict(id=str(uuid4()), createdAt=stamp, useCount=0, lastUsedAt=stamp)
            ledger["items"].append(current)
        current.update(name=name, defaultCategory=value["defaultCategory"], updatedAt=stamp)
        if method == "POST":
            current.update(useCount=current["useCount"] + 1, lastUsedAt=stamp)
        return self.save("payees", ledger)

    def remittances(self, method, record_id, body):
        ledger = self.load("money-in")
        if method == "GET" and not record_id:
            return ledger
        fields(body, ("revision", "remittance"), ("revision",))
        self.revision(ledger, body)
        current = self.find(ledger["remittances"], record_id) if record_id else None
        if method == "DELETE" and current:
            ledger["remittances"].remove(current)  # audited correction, no original deletion
        elif method == "POST" and not record_id or method == "PATCH" and current:
            value = self.payment(body.get("remittance"))
            period = next((p for p in ledger["settlements"] if p["periodEnd"] == value["periodEnd"]), None)
            if period is None or value["receivedDate"] < value["periodEnd"]:
                fail("Choose a confirmed statement and a receipt date on or after its period.")
            if current and current["periodEnd"] != value["periodEnd"]:
                fail("A remittance cannot be moved to another period.")
            if current is None:
                if len(ledger["remittances"]) >= 520 or any(p["periodEnd"] == value["periodEnd"] for p in ledger["remittances"]):
                    fail("A payment is already recorded for that period, or the record limit was reached.", 409)
                current = dict(id=str(uuid4()), createdAt=now())
                ledger["remittances"].append(current)
            current.update(value, updatedAt=now())
        else:
            fail()
        return self.save("money-in", ledger)

    def payment(self, value, cash=False):
        required = ("periodEnd", "receivedDate", "amountCents", "method", "note")
        fields(value, (*required, "customer", "storeId") if cash else required, (*required, "customer") if cash else required)
        day(value["periodEnd"])
        if day(value["receivedDate"]) > self.today or value["method"] not in RECEIPT_METHODS:
            fail("Check the receipt date and method.")
        amount(value["amountCents"])
        result = dict(value, note=text(value["note"], 500))
        if cash:
            result["customer"] = text(value["customer"], 120)
            if "storeId" in value and (not isinstance(value["storeId"], str) or not STORE_ID.fullmatch(value["storeId"])):
                fail("Choose a valid route store.")
        return result

    def route_stores(self, method, body, refresh=None):
        ledger = self.load("route-stores")
        if method == "GET":
            return ledger
        fields(body, ("revision", "action", "selectedStoreIds", "handheldStoreIds"), ("action",))
        if method != "POST":
            fail()
        if body["action"] == "refresh" and set(body) == {"action"}:
            stores = refresh()
            if type(stores) is not list or len(stores) > 1000:
                fail("Route stores could not be verified.", 503)
            ids = set()
            for store in stores:
                fields(store, ("id", "name", "number"), ("id", "name"))
                if not isinstance(store["id"], str) or not STORE_ID.fullmatch(store["id"]) or store["id"] in ids:
                    fail("Route stores could not be verified.", 503)
                text(store["name"], 120, True)
                if "number" in store:
                    text(store["number"], 60)
                ids.add(store["id"])
            ledger.update(stores=sorted(stores, key=lambda s: s["name"].lower()), refreshedAt=now())
        elif body["action"] == "select":
            self.revision(ledger, body)
            selected = body.get("selectedStoreIds")
            handheld = body.get("handheldStoreIds", ledger["handheldStoreIds"])
            for ids in (selected, handheld):
                if type(ids) is not list or len(ids) > 1000 or any(not isinstance(i, str) or not STORE_ID.fullmatch(i) for i in ids) or len(set(ids)) != len(ids):
                    fail("Choose valid route stores.")
            known = {s["id"] for s in ledger["stores"]} | set(ledger["selectedStoreIds"])
            if set(selected) - known:
                fail("Choose stores from this route.")
            ledger.update(selectedStoreIds=selected, handheldStoreIds=[i for i in handheld if i in selected])
        else:
            fail()
        return self.save("route-stores", ledger)

    def cash(self, method, suffix, body):
        ledger = self.load("cash-accounts")
        periods = self.load("money-in")["settlements"]
        if method == "GET" and not suffix:
            return dict(ledger, settlements=periods)
        parts = suffix.split("/") if suffix else []
        store_amount = bool(parts and parts[0] == "store-amounts")
        record_id = (parts[1] if len(parts) == 2 else "") if store_amount else suffix
        fields(body, ("revision", "storeAmount") if store_amount else ("revision", "action", "payment", "duplicateConfirmed"), ("revision",))
        self.revision(ledger, body)
        items = ledger["storeAmounts" if store_amount else "payments"]
        current = self.find(items, record_id) if record_id else None
        stamp = now()
        if not store_amount and method == "PATCH" and current and body.get("action") in ("archive", "restore"):
            if body["action"] == "archive":
                current["archivedAt"] = stamp
            else:
                current.pop("archivedAt", None)
            current["updatedAt"] = stamp
        elif method == "POST" and not record_id or method == "PATCH" and current and not current.get("archivedAt"):
            if store_amount:
                value = fields(body.get("storeAmount"), ("periodEnd", "customer", "amountCents", "storeId"), ("periodEnd", "customer", "amountCents"))
                day(value["periodEnd"])
                amount(value["amountCents"])
                value = dict(value, customer=text(value["customer"], 120, True))
                if "storeId" in value and (not isinstance(value["storeId"], str) or not STORE_ID.fullmatch(value["storeId"])):
                    fail("Choose a valid route store.")
            else:
                if body.get("action") != "save" or "duplicateConfirmed" in body and type(body["duplicateConfirmed"]) is not bool:
                    fail()
                value = self.payment(body.get("payment"), cash=True)
            if current and current["periodEnd"] != value["periodEnd"]:
                fail("A cash record cannot be moved to another period.")
            period = next((p for p in periods if p["periodEnd"] == value["periodEnd"]), None)
            if period is None or (not current and period["cashSalesCents"] <= 0):
                fail("Choose a confirmed statement with cash-account sales.")
            same_store = current and current.get("storeId") == value.get("storeId") and current["customer"] == value["customer"]
            if not same_store:
                directory = self.load("route-stores")
                store = next((s for s in directory["stores"] if s["id"] == value.get("storeId") and s["id"] in directory["selectedStoreIds"]), None)
                if store is None:
                    fail("Choose a cash-account store from this route.")
                value["customer"] = store["name"]
            for item in items:
                if item is current or item.get("archivedAt") or item["periodEnd"] != value["periodEnd"]:
                    continue
                same = item["storeId"] == value["storeId"] if item.get("storeId") and value.get("storeId") else key(item["customer"]) == key(value["customer"])
                if same and (store_amount or (item["receivedDate"] == value["receivedDate"] and item["amountCents"] == value["amountCents"] and body.get("duplicateConfirmed") is not True)):
                    fail("This store amount already exists." if store_amount else "A matching collection exists. Confirm the duplicate before saving.", 409)
            if current is None:
                if len(items) >= 10000:
                    fail("Cash-account record limit reached.", 409)
                current = dict(id=str(uuid4()), createdAt=stamp)
                items.append(current)
            current.update(value, updatedAt=stamp)
        else:
            fail("Invalid cash-account change.")
        return dict(self.save("cash-accounts", ledger), settlements=periods)
