from abc import ABC, abstractmethod
from concurrent.futures import as_completed
from enum import Enum
from traceback import format_exc
from typing import TYPE_CHECKING, Dict

from snowddl.blueprint import AbstractBlueprint, DependsOnMixin, Edition, ObjectType
from snowddl.error import SnowDDLExecuteError, SnowDDLUnsupportedError

if TYPE_CHECKING:
    from snowddl.engine import SnowDDLEngine


class ResolveResult(Enum):
    CREATE = "CREATE"
    ALTER = "ALTER"
    DROP = "DROP"
    REPLACE = "REPLACE"
    SKIP = "SKIP"
    GRANT = "GRANT"
    NOCHANGE = "NOCHANGE"
    ERROR = "ERROR"
    UNSUPPORTED = "UNSUPPORTED"


class AbstractResolver(ABC):
    skip_on_empty_blueprints = False
    skip_min_edition = Edition.STANDARD

    def __init__(self, engine: "SnowDDLEngine"):
        self.engine = engine
        self.config = engine.config

        self.object_type = self.get_object_type()
        self.blueprints: Dict[str, AbstractBlueprint] = {}
        self.existing_objects: Dict[str, Dict] = {}

        self.resolved_objects: Dict[str, ResolveResult] = {}
        self.errors: Dict[str, Exception] = {}

    def resolve(self):
        if self._is_skipped():
            return

        self.blueprints = self.get_blueprints()

        try:
            self.existing_objects = self.get_existing_objects()
        except SnowDDLExecuteError as e:
            self.engine.logger.info(
                f"Could not get existing objects for resolver [{self.__class__.__name__}]: \n{e.verbose_message()}"
            )
            raise e.snow_exc

        self._pre_process()
        self._resolve_create_compare()
        self._resolve_drop()
        self._post_process()

    def destroy(self):
        if self._is_skipped():
            return

        try:
            self.existing_objects = self.get_existing_objects()
        except SnowDDLExecuteError as e:
            self.engine.logger.info(
                f"Could not get existing objects for resolver [{self.__class__.__name__}]: \n{e.verbose_message()}"
            )
            raise e.snow_exc

        self._pre_process()
        self._destroy_drop()
        self._post_process()

    def _resolve_create_compare(self):
        # Process blueprints in batches sorted in order of dependencies
        for blueprint_names_batch in self._split_blueprints_into_batches():
            tasks = {
                full_name: (
                    self.compare_object,
                    self.blueprints[full_name],
                    self.existing_objects[full_name],
                )
                if full_name in self.existing_objects
                else (self.create_object, self.blueprints[full_name])
                for full_name in sorted(blueprint_names_batch)
            }


            self._process_tasks(tasks)

    def _resolve_drop(self):
        # Drop existing objects without blueprints
        tasks = {
            full_name: (self.drop_object, self.existing_objects[full_name])
            for full_name in sorted(self.existing_objects)
            if full_name not in self.blueprints
        }


        self._process_tasks(tasks)

    def _destroy_drop(self):
        tasks = {
            full_name: (self.drop_object, self.existing_objects[full_name])
            for full_name in sorted(self.existing_objects)
        }


        self._process_tasks(tasks)

    def _process_tasks(self, tasks):
        futures = {
            self.engine.executor.submit(*args): full_name
            for full_name, args in tasks.items()
        }


        for f in as_completed(futures):
            full_name = futures[f]

            try:
                result = f.result()
                if result == ResolveResult.NOCHANGE:
                    self.engine.logger.debug(
                        f"Resolved {self.object_type.name} [{full_name}]: {result.value}"
                    )
                else:
                    self.engine.logger.info(
                        f"Resolved {self.object_type.name} [{full_name}]: {result.value}"
                    )
            except Exception as e:
                if isinstance(e, SnowDDLUnsupportedError):
                    result = ResolveResult.UNSUPPORTED
                else:
                    result = ResolveResult.ERROR

                if isinstance(e, SnowDDLExecuteError):
                    error_text = e.verbose_message()
                else:
                    error_text = format_exc()

                self.engine.logger.warning(
                    f"Resolved {self.object_type.name} [{full_name}]: {result.value}\n{error_text}"
                )
                self.errors[full_name] = e

            self.resolved_objects[full_name] = result

        self.engine.flush_thread_buffers()

    def _split_blueprints_into_batches(self):
        all_batches = []
        allocated_full_names = set()

        remaining_blueprints = self.blueprints.copy()

        # Create new batches as long as at least one unallocated blueprint remains
        while remaining_blueprints:
            batch = [
                full_name
                for full_name, bp in remaining_blueprints.items()
                if (
                    not isinstance(bp, DependsOnMixin)
                    or not bp.depends_on
                    or all((str(d) in allocated_full_names) for d in bp.depends_on)
                )
            ]


            # Allocate blueprints with unresolved dependencies to the last batch
            if not batch:
                batch.extend(iter(remaining_blueprints))
            # Forget blueprints allocated during this iteration
            for full_name in batch:
                del remaining_blueprints[full_name]

            all_batches.append(batch)
            allocated_full_names.update(batch)

        return all_batches

    def _is_skipped(self):
        if self.engine.context.edition < self.skip_min_edition:
            return True

        if self.object_type in self.engine.settings.exclude_object_types:
            return True

        if self.engine.settings.include_object_types:
            return self.object_type not in self.engine.settings.include_object_types

        return bool(self.skip_on_empty_blueprints and not self.get_blueprints())

    def _pre_process(self):
        pass

    def _post_process(self):
        pass

    @abstractmethod
    def get_object_type(self) -> ObjectType:
        pass

    @abstractmethod
    def get_blueprints(self) -> Dict[str, AbstractBlueprint]:
        pass

    @abstractmethod
    def get_existing_objects(self) -> Dict[str, Dict]:
        pass

    @abstractmethod
    def create_object(self, bp: AbstractBlueprint) -> ResolveResult:
        pass

    @abstractmethod
    def compare_object(self, bp: AbstractBlueprint, row: Dict) -> ResolveResult:
        pass

    @abstractmethod
    def drop_object(self, row: Dict):
        pass
