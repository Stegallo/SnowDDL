from pathlib import Path
from shutil import rmtree

from snowddl.app.base import BaseApp
from snowddl.blueprint import DatabaseIdent, ObjectType
from snowddl.config import SnowDDLConfig
from snowddl.converter import default_converter_sequence
from snowddl.settings import SnowDDLSettings


class ConvertApp(BaseApp):
    def init_config_path(self):
        config_path = Path(self.args["c"])

        if config_path.is_dir() and self.args.get("clean"):
            rmtree(config_path)

        if not config_path.exists():
            config_path.mkdir(mode=0o755, parents=True)

        if not config_path.is_dir():
            raise ValueError(f"Config path [{self.args.path}] is not a directory")

        return config_path

    def init_config(self):
        return SnowDDLConfig(self.args.get("env_prefix"))

    def init_settings(self):
        settings = SnowDDLSettings()

        if self.args.get("exclude_object_types"):
            try:
                settings.exclude_object_types = [
                    ObjectType[t.strip().upper()]
                    for t in str(self.args.get("exclude_object_types")).split(",")
                ]
            except KeyError as e:
                raise ValueError(f"Invalid object type [{str(e)}]")

        if self.args.get("include_object_types"):
            try:
                settings.include_object_types = [
                    ObjectType[t.strip().upper()]
                    for t in str(self.args.get("include_object_types")).split(",")
                ]
            except KeyError as e:
                raise ValueError(f"Invalid object type [{str(e)}]")

        if self.args.get("include_databases"):
            settings.include_databases = [
                DatabaseIdent(self.config.env_prefix, d)
                for d in str(self.args.get("include_databases")).split(",")
            ]

        if self.args.get("ignore_ownership"):
            settings.ignore_ownership = True

        if self.args.get("max_workers"):
            settings.max_workers = int(self.args.get("max_workers"))

        return settings

    def execute(self):
        error_count = 0

        with self.engine:
            self.output_engine_context()

            for converter_cls in default_converter_sequence:
                converter = converter_cls(self.engine, self.config_path)
                converter.convert()

                error_count += len(converter.errors)

            if error_count > 0:
                exit(8)


def entry_point():
    app = ConvertApp()
    app.execute()
