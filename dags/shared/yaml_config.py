import yaml
import pprint
from datetime import timedelta


class DagConfig:
    """An object to hold and enhance the config file for a dag.

    Get or set all attributes for the object via ['<attribute_name']. Getting any
    attribute that does not exist will return `None`.
    """

    # Add config key to POSSIBLE_TAGS to add a tag
    POSSIBLE_TAGS = {  # account_name for postgres added in self._make_tags()
        "large_dataset",
        "migrated",
        "upload_to_carto",
        "upload_to_ago",
        "managed_share",
        "opendata_ago_refresh",
        "opendata_s3_upload",
        "dbt_projects",
        "dbt_project_source_tests",
        "track_history",
        "primary_keys",
        "reproject_viewer_to_srid",
    }
    # Also use TAG_MAPPING if you want the tag to be different from its config key
    TAG_MAPPING = {
        "upload_to_carto": "carto",
        "upload_to_ago": "ago",
        "dbt_projects": "dbt",
        "dbt_project_source_tests": "dbt",
        "primary_keys": "primary_key",
        "reproject_viewer_to_srid": "reproject_srid",
    }

    DEFAULT_TIMEOUT = 50

    def __init__(self, d: dict = None):
        """d (dict): dict of config values to pass"""
        self._config = d
        # Special: if migrated wasn't specified in the dag config, default to True. We want "migrated" to be considered the default state.
        # False values will be treated normally.
        if self["migrated"] == None:
            self["migrated"] = True

        # if upload_to_ago is true, always set share_privileges to "ago" so it gets this marker role
        # and is always copied to 'viewer'.

        if self["upload_to_ago"]:
            if self["share_privileges"]:
                self["share_privileges"] += ",ago"
            else:
                self["share_privileges"] = "ago"
        if self["upload_to_carto"]:
            if self["share_privileges"]:
                self["share_privileges"] += ",carto"
            else:
                self["share_privileges"] = "carto"

        # Strip spaces from comma-separated share_privileges
        if self["share_privileges"]:
            self["share_privileges"] = ",".join(
                [x.strip() for x in self["share_privileges"].split(",")]
            )

        self["dag_id"] = self._make_dag_id()
        self["source_schema"] = self._make_source_schema()
        self["execution_timeout"] = self._make_execution_timeout()
        self["tags"] = self._make_tags()

    def __getitem__(self, key):
        """Control what `DagConfig[key]` does"""
        try:
            return self._config[key]
        except KeyError:
            return None

    def __setitem__(self, key, value):
        """Control what `DagConfig[key] = value` does"""
        self._config[key] = value

    def __str__(self):
        """Control what `print(DagConfig)` does"""
        return self.__repr__()

    def __repr__(self):
        """Control what `DagConfig` does"""
        try:  # Only available on python 3.8+
            return f'Configuration for DAG ID "{self["dag_id"]}":\n{pprint.pformat(self._config, sort_dicts=False)}'
        except TypeError:
            return f'Configuration for DAG ID "{self["dag_id"]}":\n{pprint.pformat(self._config)}'

    def _make_tags(self) -> list:
        """Set tags based on options for easier viewing/filtering in airflow web UI"""
        l = []
        l.append(self["account_name"])
        for key, value in self._config.items():
            if key == "ago_access" and value == "public":
                l.append("ago_public_access")
            if key == "ago_access" and (value == "enterprise" or value == "citywide"):
                l.append("ago_enterprise_access")
            # If key exists and is not null
            if key == "share_privileges" and value:
                if len(value.split(",")) > 1:
                    for i in value.split(","):
                        l.append("share_to_" + i.strip())
                elif isinstance(value, list):
                    for i in value:
                        l.append("share_to_" + i.strip())
                else:
                    l.append("share_to_" + value.strip())
            if key in self.POSSIBLE_TAGS and value:
                if key in self.TAG_MAPPING:
                    l.append(self.TAG_MAPPING[key])
                else:
                    l.append(key)
        return l

    def _make_dag_id(self) -> str:
        """Set the actual DAG name that will show up in the UI."""
        if self["migrated"]:
            account = self["account_name"]
        else:
            account = self["oracle_account_name"]
        if account != None:
            return f"{account.lower()}__{self['table_name'].lower()}"
        else:
            return f"__{self['table_name'].lower()}"

    def _make_source_schema(self) -> str:
        """Set the source schema for sharing datasets, using the form viewer_<dept>
        so that we can have private and citywide shares.
        """
        if self["share_privileges"]:
            if self["override_viewer_account"]:
                s = "viewer_" + self["override_viewer_account"].replace("viewer_", "")
            else:
                s = "viewer_" + self["account_name"]
        else:
            s = self["account_name"]

        # If dept_managed_etl then reset source schema to their own schema.
        if self["dept_managed_etl"]:
            s = self["account_name"]

        return s

    def _make_execution_timeout(self):
        # Default timeout for every task in the DAG, which will fail it if it goes over this amount.
        if not self["execution_timeout"]:
            return timedelta(minutes=self.DEFAULT_TIMEOUT)
        else:
            return timedelta(minutes=int(self["execution_timeout"]))

    # Return the inner dictionary
    def to_dict(self):
        return self._config.copy()


def get_yaml_data(file_path: str, encoding: str = "utf-8"):
    with open(file_path, "rt", encoding=encoding) as f:
        return yaml.safe_load(f)
