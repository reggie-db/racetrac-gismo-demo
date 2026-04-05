from collections.abc import Sequence

from common.gismo import (
    BRONZE_TABLES as COMMON_BRONZE_TABLES,
)
from common.gismo import (
    GOLD_TABLES as COMMON_GOLD_TABLES,
)
from common.gismo import (
    SILVER_TABLES as COMMON_SILVER_TABLES,
)

# Re-exports shared GISMO table name collections for pipeline modules.

BRONZE_TABLES: Sequence[str] = COMMON_BRONZE_TABLES
SILVER_TABLES: Sequence[str] = COMMON_SILVER_TABLES
GOLD_TABLES: Sequence[str] = COMMON_GOLD_TABLES
