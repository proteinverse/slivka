from datetime import datetime

from packaging.specifiers import SpecifierSet
from packaging.version import Version

name = "Timezone aware timestamps"
from_versions = SpecifierSet("<0.8.5b4", prereleases=True)
to_version = Version("0.8.5b4")
optional = False

def apply():
    import slivka.db
    import slivka.db.documents
    requests_collection = slivka.db.database['requests']
    offset = datetime.now().astimezone().utcoffset().total_seconds()
    ms_offset = int(offset * 1000)
    requests_collection.update_many(
        {"timestamp": {"$exists": True}},
        [{"$set": {"timestamp": {"$subtract": ["$timestamp", ms_offset]}}}]
    )
    requests_collection.update_many(
        {"completion_time": {"$exists": True}},
        [{"$set": {"completion_time": {"$subtract": ["$completion_time", ms_offset]}}}]
    )
