from enum import Enum
from json import loads as json_loads
from json.decoder import JSONDecodeError
from logging import Formatter, StreamHandler, getLogger
from os import getcwd
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel
from snowflake.connector import connect

from snowddl.blueprint import ObjectType
from snowddl.config import SnowDDLConfig
from snowddl.engine import SnowDDLEngine
from snowddl.parser import PlaceholderParser, default_parser_sequence
from snowddl.resolver import default_resolver_sequence
from snowddl.settings import SnowDDLSettings
from snowddl.version import __version__


class ActionEnum(str, Enum):
    plan = "plan"
    apply = "apply"
    destroy = "destroy"


class Configuration(BaseModel):
    # Config
    path: Optional[str] = getcwd()
    # Auth
    account: str
    user: str
    password: str
    private_key: Optional[str]
    # Role & warehouse
    role: str
    warehouse: str
    # Options
    passphrase: Optional[str]
    env_prefix: Optional[str]
    max_workers: Optional[str]
    # Logging
    log_level: Optional[str] = "INFO"
    show_sql: Optional[bool] = False
    # Placeholders
    placeholder_path: Optional[str] = False
    placeholder_values: Optional[str]
    # Object types
    exclude_object_types: Optional[List[str]]
    include_object_types: Optional[List[str]]
    # Apply even more unsafe changes
    apply_unsafe: Optional[bool] = False
    apply_replace_table: Optional[bool] = False
    apply_masking_policy: Optional[bool] = False
    apply_row_access_policy: Optional[bool] = False
    apply_account_params: Optional[bool] = False
    apply_network_policy: Optional[bool] = False
    apply_resource_monitor: Optional[bool] = False
    apply_outbound_share: Optional[bool] = False
    # Refresh state of specific objects
    refresh_user_passwords: Optional[bool] = False
    refresh_future_grants: Optional[bool] = False
    # Destroy without env prefix
    destroy_without_prefix: Optional[bool] = False

    action: ActionEnum


class BaseApp:
    parser_sequence = default_parser_sequence
    resolver_sequence = default_resolver_sequence

    def __init__(self, config):
        self.args = self.init_arguments(config)

        self.logger = self.init_logger()

        self.config_path = self.init_config_path()
        self.config = self.init_config()
        self.settings = self.init_settings()

        self.engine = self.init_engine()

    def init_arguments(self, config):
        args = config

        if (
            not args.account
            or not args.user
            or (not args.password and not args.private_key)
        ):
            raise Exception

        return args

    def init_logger(self):
        logger = getLogger("snowddl")
        logger.setLevel(self.args.log_level)

        formatter = Formatter("%(asctime)s - %(levelname)s - %(message)s")
        formatter.default_msec_format = "%s.%03d"

        handler = StreamHandler()
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        return logger

    def init_config_path(self):
        config_path = Path(self.args.path)

        if not config_path.exists():
            config_path = Path(__file__).parent.parent / "_config" / self.args.path

        if not config_path.exists():
            raise ValueError(f"Config path [{self.args.path}] does not exist")

        if not config_path.is_dir():
            raise ValueError(f"Config path [{self.args.path}] is not a directory")

        return config_path.resolve()

    def init_config(self):
        config = SnowDDLConfig(self.args.env_prefix)

        # Placeholders
        placeholder_path = self.get_placeholder_path()
        placeholder_values = self.get_placeholder_values()

        parser = PlaceholderParser(config, self.config_path)
        parser.load_placeholders(placeholder_path, placeholder_values)

        if config.errors:
            self.output_config_errors(config)
            exit(1)

        # Blueprints
        for parser_cls in self.parser_sequence:
            parser = parser_cls(config, self.config_path)
            parser.load_blueprints()

        if config.errors:
            self.output_config_errors(config)
            exit(1)

        return config

    def init_settings(self):
        settings = SnowDDLSettings()

        if self.args.action in ("apply", "destroy"):
            settings.execute_safe_ddl = True

            if self.args.apply_unsafe or self.args.action == "destroy":
                settings.execute_unsafe_ddl = True

            if self.args.apply_replace_table:
                settings.execute_replace_table = True

            if self.args.apply_masking_policy:
                settings.execute_masking_policy = True

            if self.args.apply_row_access_policy:
                settings.execute_row_access_policy = True

            if self.args.apply_account_params:
                settings.execute_account_params = True

            if self.args.apply_network_policy:
                settings.execute_network_policy = True

            if self.args.apply_resource_monitor:
                settings.execute_resource_monitor = True

            if self.args.apply_inbound_share:
                settings.execute_inbound_share = True

            if self.args.apply_outbound_share:
                settings.execute_outbound_share = True

        if self.args.refresh_user_passwords:
            settings.refresh_user_passwords = True

        if self.args.refresh_future_grants:
            settings.refresh_future_grants = True

        if self.args.exclude_object_types:
            try:
                settings.exclude_object_types = [
                    ObjectType[t.strip().upper()]
                    for t in str(self.args.exclude_object_types).split(",")
                ]
            except KeyError as e:
                raise ValueError(f"Invalid object type [{str(e)}]")

        if self.args.include_object_types:
            try:
                settings.include_object_types = [
                    ObjectType[t.strip().upper()]
                    for t in str(self.args.include_object_types).split(",")
                ]
            except KeyError as e:
                raise ValueError(f"Invalid object type [{str(e)}]")

        if self.args.max_workers:
            settings.max_workers = int(self.args.max_workers)

        return settings

    def init_engine(self):
        return SnowDDLEngine(self.get_connection(), self.config, self.settings)

    def get_connection(self):
        options = {
            "account": self.args.account,
            "user": self.args.user,
            "role": self.args.role,
            "warehouse": self.args.warehouse,
        }

        if self.args.private_key:
            from cryptography.hazmat.primitives import serialization

            key_path = Path(self.args.private_key)
            key_password = (
                str(self.args.passphrase).encode("utf-8")
                if self.args.passphrase
                else None
            )

            pk = serialization.load_pem_private_key(
                data=key_path.read_bytes(), password=key_password
            )

            options["private_key"] = pk.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        else:
            options["password"] = self.args.password

        return connect(**options)

    def execute(self):
        error_count = 0

        with self.engine:
            self.output_engine_context()

            if self.args.action == "destroy":
                if not self.args.env_prefix and not self.args.get(
                    "destroy_without_prefix"
                ):
                    raise ValueError(
                        "Argument --env-prefix is required for [destroy] action"
                    )

                for resolver_cls in self.resolver_sequence:
                    resolver = resolver_cls(self.engine)
                    resolver.destroy()

                    error_count += len(resolver.errors)

                self.engine.context.destroy_role_with_prefix()
            else:
                for resolver_cls in self.resolver_sequence:
                    resolver = resolver_cls(self.engine)
                    resolver.resolve()

                    error_count += len(resolver.errors)

            self.output_engine_stats()

            if self.args.show_sql:
                self.output_executed_ddl()

            self.output_suggested_ddl()

            if error_count > 0:
                exit(8)

    def output_engine_context(self):
        roles = []

        if self.engine.context.is_account_admin:
            roles.append("ACCOUNTADMIN")

        if self.engine.context.is_sys_admin:
            roles.append("SYSADMIN")

        if self.engine.context.is_security_admin:
            roles.append("SECURITYADMIN")

        self.logger.info(
            f"Snowflake version = {self.engine.context.version}"
            f"({self.engine.context.edition.name}), "
            f"SnowDDL version = {__version__}"
        )
        self.logger.info(
            f"Session = {self.engine.context.current_session}, "
            f"User = {self.engine.context.current_user}"
        )
        self.logger.info(
            f"Role = {self.engine.context.current_role}, "
            f"Warehouse = {self.engine.context.current_warehouse}"
        )
        self.logger.info(f"Roles in session = {','.join(roles)}")
        self.logger.info("---")

    def get_placeholder_path(self):
        if self.args.placeholder_path:
            placeholder_path = Path(self.args.placeholder_path)

            if not placeholder_path.is_file():
                raise ValueError(
                    f"Placeholder path [{self.args.get('placeholder_path')}]"
                    " does not exist or not a file"
                )

            return placeholder_path.resolve()

        return None

    def get_placeholder_values(self):
        if self.args.placeholder_values:
            try:
                placeholder_values = json_loads(self.args.placeholder_values)
            except JSONDecodeError:
                raise ValueError(
                    f"Placeholder values [{self.args.get('placeholder_values')}] "
                    "are not a valid JSON"
                )

            if not isinstance(placeholder_values, dict):
                raise ValueError(
                    f"Placeholder values [{self.args.get('placeholder_values')}] "
                    "are not JSON encoded dict"
                )

            for k, v in placeholder_values.items():
                if not isinstance(v, (bool, float, int, str)):
                    raise ValueError(
                        f"Invalid type [{type(v)}] of placeholder [{k.upper()}] "
                        f"value, supported types are: bool, float, int, str"
                    )

            return placeholder_values

        return None

    def output_config_errors(self, config):
        for e in config.errors:
            self.logger.warning(
                f"[{e['path']}]:"
                "{''.join(TracebackException.from_exception(e['error']).format())}"
            )

    def output_engine_stats(self):
        self.logger.info(
            f"Executed {len(self.engine.executed_ddl)} DDL queries, "
            f"Suggested {len(self.engine.suggested_ddl)} DDL queries"
        )

    def output_suggested_ddl(self):
        if self.engine.suggested_ddl:
            print("--- Suggested DDL ---\n")

        for sql in self.engine.suggested_ddl:
            print(f"{sql};\n")

    def output_executed_ddl(self):
        if self.engine.executed_ddl:
            print("--- Executed DDL ---\n")

        for sql in self.engine.executed_ddl:
            print(f"{sql};\n")


def entry_point():
    app = BaseApp()
    app.execute()
