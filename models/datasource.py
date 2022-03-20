import json


class Datasource:
    name: str = None
    format: str = None
    source_path: str = None
    dest_path_landing_temp: str = None
    dest_path_landing: str = None

    def __init__(self, aux: dict):
        self.name = aux['name']
        self.format = aux['format']
        self.source_path = aux['source_path']
        self.dest_path_landing = aux['dest_path_landing']
        self.dest_path_landing_temp = aux['dest_path_landing_temp']

    def to_dict(self):
        return self.__dict__

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)
