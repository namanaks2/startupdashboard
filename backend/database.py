"""
Database layer — uses a simple in-memory store when Supabase is not configured.
In production, swap the in-memory dict for actual Supabase client calls.
"""
import uuid
from datetime import datetime
from typing import Optional

# ── In-memory store (dev / demo) ──────────────────────────────────────────────
_users: dict[str, dict] = {}          # email → user record
_datasets: dict[str, list] = {}       # user_id → list of dataset dicts


# ── User helpers ──────────────────────────────────────────────────────────────

def create_user(email: str, hashed_password: str, name: str, role: str = "user") -> dict:
    user = {
        "id": str(uuid.uuid4()),
        "email": email,
        "name": name,
        "hashed_password": hashed_password,
        "role": role,
        "created_at": datetime.utcnow().isoformat(),
    }
    _users[email] = user
    return user


def get_user_by_email(email: str) -> Optional[dict]:
    return _users.get(email)


def get_user_by_id(user_id: str) -> Optional[dict]:
    for u in _users.values():
        if u["id"] == user_id:
            return u
    return None


# ── Dataset helpers ───────────────────────────────────────────────────────────

def save_dataset(user_id: str, dataset: dict) -> dict:
    dataset["id"] = str(uuid.uuid4())
    dataset["user_id"] = user_id
    dataset["created_at"] = datetime.utcnow().isoformat()
    _datasets.setdefault(user_id, []).append(dataset)
    return dataset


def get_datasets_for_user(user_id: str) -> list[dict]:
    return _datasets.get(user_id, [])


def get_dataset_by_id(user_id: str, dataset_id: str) -> Optional[dict]:
    for ds in _datasets.get(user_id, []):
        if ds["id"] == dataset_id:
            return ds
    return None


def delete_dataset(user_id: str, dataset_id: str) -> bool:
    datasets = _datasets.get(user_id, [])
    new_list = [d for d in datasets if d["id"] != dataset_id]
    if len(new_list) == len(datasets):
        return False
    _datasets[user_id] = new_list
    return True
