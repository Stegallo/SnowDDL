from snowddl.blueprint import Grant, RoleBlueprint, WarehouseBlueprint, build_role_ident
from snowddl.resolver.abc_role_resolver import AbstractRoleResolver, ObjectType


class WarehouseRoleResolver(AbstractRoleResolver):
    def get_role_suffix(self):
        return self.config.WAREHOUSE_ROLE_SUFFIX

    def get_blueprints(self):
        blueprints = []

        for warehouse in self.config.get_blueprints_by_type(
            WarehouseBlueprint
        ).values():
            blueprints.extend(
                (
                    self.get_blueprint_usage_role(warehouse),
                    self.get_blueprint_monitor_role(warehouse),
                )
            )

        return {str(bp.full_name): bp for bp in blueprints}

    def get_blueprint_usage_role(self, warehouse: WarehouseBlueprint):
        grants = [
            Grant(
                privilege="USAGE",
                on=ObjectType.WAREHOUSE,
                name=warehouse.full_name,
            )
        ]


        grants.append(
            Grant(
                privilege="OPERATE",
                on=ObjectType.WAREHOUSE,
                name=warehouse.full_name,
            )
        )

        return RoleBlueprint(
            full_name=build_role_ident(
                self.config.env_prefix,
                warehouse.full_name,
                "USAGE",
                self.get_role_suffix(),
            ),
            grants=grants,
            future_grants=[],
            comment=None,
        )

    def get_blueprint_monitor_role(self, warehouse: WarehouseBlueprint):
        grants = [
            Grant(
                privilege="MONITOR",
                on=ObjectType.WAREHOUSE,
                name=warehouse.full_name,
            )
        ]


        grants.append(
            Grant(
                privilege="OPERATE",
                on=ObjectType.WAREHOUSE,
                name=warehouse.full_name,
            )
        )

        return RoleBlueprint(
            full_name=build_role_ident(
                self.config.env_prefix,
                warehouse.full_name,
                "MONITOR",
                self.get_role_suffix(),
            ),
            grants=grants,
            future_grants=[],
            comment=None,
        )
