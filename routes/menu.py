from fastapi import APIRouter, HTTPException, Body
from pymongo import MongoClient
from typing import List, Optional
import os

router = APIRouter()

# Mongo setup
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client["powercasting"]
menu_coll = db["menu_permissions"]
overrides_coll = db["user_overrides"]


# ----------------- MENU CRUD ----------------- #

@router.get("/", summary="Get all menu items")
def get_all_menus():
    return list(menu_coll.find({}, {"_id": 0}))


@router.post("/", summary="Create a new menu item")
def create_menu(item: dict = Body(...)):
    res = menu_coll.insert_one(item)
    return {"message": "Menu item created", "inserted_id": str(res.inserted_id)}


@router.post("/bulk", summary="Insert multiple menu items")
def create_menus(items: List[dict] = Body(...)):
    if not isinstance(items, list):
        raise HTTPException(400, "Expected a list of menu items")
    res = menu_coll.insert_many(items)
    return {
        "message": f"{len(res.inserted_ids)} items inserted",
        "inserted_ids": [str(i) for i in res.inserted_ids]
    }


@router.put("/{title}", summary="Update menu by title")
def update_menu(title: str, item: dict = Body(...)):
    res = menu_coll.update_one({"title": title}, {"$set": item})
    if res.matched_count == 0:
        raise HTTPException(404, "Menu not found")
    return {"message": "Menu updated"}


@router.delete("/{title}", summary="Delete menu by title")
def delete_menu(title: str):
    res = menu_coll.delete_one({"title": title})
    if res.deleted_count == 0:
        raise HTTPException(404, "Menu not found")
    return {"message": "Menu deleted"}


# ----------------- MENU EXTRA OPS ----------------- #

@router.get("/titles", summary="Get all menu titles")
def get_titles():
    return [m["title"] for m in menu_coll.find({}, {"title": 1, "_id": 0})]


@router.get("/paths", summary="Get all submenu paths")
def get_paths():
    paths = []

    def collect(item):
        if "path" in item:
            paths.append(item["path"])
        for s in item.get("submenu", []):
            collect(s)

    for m in menu_coll.find({}, {"_id": 0}):
        collect(m)
    return sorted(set(paths))


@router.get("/search", summary="Search menu by title")
def search_menu(q: str):
    cursor = menu_coll.find({"title": {"$regex": q, "$options": "i"}}, {"_id": 0})
    return list(cursor)


# ----------------- USER OVERRIDES CRUD ----------------- #

def _build_query(user_id: Optional[int] = None, email: Optional[str] = None):
    if user_id:
        return {"user_id": user_id}
    if email:
        return {"email": email}
    raise HTTPException(400, "Either user_id or email required")


@router.get("/overrides/id/{user_id}", summary="Get overrides by user_id")
def get_overrides_by_id(user_id: int):
    doc = overrides_coll.find_one({"user_id": user_id}, {"_id": 0})
    return doc or {"user_id": user_id, "overrides": []}


@router.get("/overrides/email/{email}", summary="Get overrides by email")
def get_overrides_by_email(email: str):
    doc = overrides_coll.find_one({"email": email}, {"_id": 0})
    return doc or {"email": email, "overrides": []}


@router.post("/overrides", summary="Set overrides (replace)")
def set_overrides(
        user_id: Optional[int] = None,
        email: Optional[str] = None,
        overrides: List[dict] = Body(...)
):
    query = _build_query(user_id, email)
    payload = {"user_id": user_id, "email": email, "overrides": overrides}
    overrides_coll.update_one(query, {"$set": payload}, upsert=True)
    return {"message": "Overrides set", **payload}


@router.patch("/overrides", summary="Patch overrides (merge)")
def patch_overrides(
        user_id: Optional[int] = None,
        email: Optional[str] = None,
        overrides: List[dict] = Body(...)
):
    query = _build_query(user_id, email)
    doc = overrides_coll.find_one(query) or {"overrides": []}
    override_map = {o["path"]: o["allowed"] for o in doc["overrides"]}
    for o in overrides:
        override_map[o["path"]] = o["allowed"]
    new_overrides = [{"path": k, "allowed": v} for k, v in override_map.items()]
    overrides_coll.update_one(query, {"$set": {"overrides": new_overrides}}, upsert=True)
    return {"message": "Overrides merged", "overrides": new_overrides}


@router.delete("/overrides", summary="Delete single override")
def delete_override(path: str, user_id: Optional[int] = None, email: Optional[str] = None):
    query = _build_query(user_id, email)
    res = overrides_coll.update_one(query, {"$pull": {"overrides": {"path": path}}})
    if res.modified_count == 0:
        raise HTTPException(404, "Override not found")
    return {"message": f"Override for {path} removed"}


@router.delete("/overrides/reset", summary="Delete all overrides for user")
def reset_overrides(user_id: Optional[int] = None, email: Optional[str] = None):
    query = _build_query(user_id, email)
    res = overrides_coll.delete_one(query)
    if res.deleted_count == 0:
        raise HTTPException(404, "No overrides found")
    return {"message": "All overrides reset", **query}


@router.get("/overrides/check", summary="Check override for path")
def check_override(path: str, user_id: Optional[int] = None, email: Optional[str] = None):
    query = _build_query(user_id, email)
    doc = overrides_coll.find_one(query, {"_id": 0})
    if not doc:
        return {"path": path, "allowed": None}
    match = next((o for o in doc.get("overrides", []) if o["path"] == path), None)
    return {"path": path, "allowed": match["allowed"] if match else None}


# ----------------- MERGED MENU ----------------- #

def _get_merged_menu(query: dict):
    menus = list(menu_coll.find({}, {"_id": 0}))
    overrides_doc = overrides_coll.find_one(query, {"_id": 0}) or {"overrides": []}
    overrides = {o["path"]: o["allowed"] for o in overrides_doc.get("overrides", [])}

    def apply_overrides(item):
        if "submenu" in item:
            item["submenu"] = [apply_overrides(s) for s in item["submenu"]]
        if item.get("path") in overrides:
            item["forced"] = overrides[item["path"]]
        return item

    return [apply_overrides(m) for m in menus]


@router.get("/user/id/{user_id}", summary="Get merged menu for user_id")
def get_user_menu_by_id(user_id: int):
    return _get_merged_menu({"user_id": user_id})


@router.get("/user/email/{email}", summary="Get merged menu for email")
def get_user_menu_by_email(email: str):
    return _get_merged_menu({"email": email})
