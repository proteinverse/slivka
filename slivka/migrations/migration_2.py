from datetime import datetime, timezone

from packaging.specifiers import SpecifierSet
from packaging.version import Version

name = "Timezone aware timestamps"
from_versions = SpecifierSet("<0.8.5b4", prereleases=True)
to_version = Version("0.8.5b4")
optional = False

def apply():
    import slivka.db
    requests_collection = slivka.db.database['requests']
    for doc in requests_collection.find({"timestamp": {"$exists": True}}):
        new_timestamp = doc["timestamp"].astimezone(timezone.utc)
        new_completion_time = (
            doc["completion_time"].astimezone(timezone.utc)
            if doc.get("completion_time") is not None
            else None
        )
        requests_collection.update_one(
            {"_id": doc["_id"]},
            { "$set": {
                "timestamp": new_timestamp,
                "completion_time": new_completion_time
            }}
        )
