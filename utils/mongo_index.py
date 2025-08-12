# utils/mongo_index.py
from pymongo.errors import OperationFailure


def ensure_index(coll, keys, name: str, *, unique: bool | None = None, drop_if_mismatch: bool = False):
    """
    Create an index if it doesn't exist. If an index with the same name exists
    but differs (e.g., unique vs non-unique), optionally drop & recreate.
    """
    info = coll.index_information()
    if name in info:
        spec = info[name]
        existing_keys = [(k, d) for k, d in spec["key"]]
        desired_keys = keys
        existing_unique = bool(spec.get("unique", False))
        desired_unique = bool(unique) if unique is not None else False

        if existing_keys == desired_keys and existing_unique == desired_unique:
            return  # already correct

        if drop_if_mismatch:
            try:
                coll.drop_index(name)
            except OperationFailure:
                pass
        else:
            return  # mismatch but do nothing to avoid startup crash

    coll.create_index(keys, name=name, unique=(unique or False))
