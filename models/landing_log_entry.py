import json


# Repeated code -  TmplandingLog
class LandingLogEntry:
    file_name: str = None
    timestamp: float = None
    source_name: str = None
    include_files: list = []

    def __init__(self, file_name, timestamp, source_name, include_files):
        self.timestamp = timestamp
        self.file_name = file_name
        self.source_name = source_name
        self.include_files = include_files

    def to_dict(self):
        return self.__dict__

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)
