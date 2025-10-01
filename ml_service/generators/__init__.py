from .base_generator import BaseDDLGenerator
from .postgresql_ddl import PostgreSQLDDLGenerator
from .clickhouse_ddl import ClickHouseDDLGenerator
from .hdfs_ddl import HDFSDDLGenerator
from ..models.schemas import StorageType


def get_ddl_generator(storage_type: StorageType) -> BaseDDLGenerator:
    generators = {
        StorageType.POSTGRESQL: PostgreSQLDDLGenerator,
        StorageType.CLICKHOUSE: ClickHouseDDLGenerator,
        StorageType.HDFS: HDFSDDLGenerator
    }
    return generators[storage_type]()