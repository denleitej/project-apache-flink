import os
import time
import pandas as pd

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings
)

from pyflink.common.typeinfo import Types

# ============================================================================
# PATHS
# ============================================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

CSV_PATH = os.path.join(
    BASE_DIR,
    "data",
    "processed",
    "pix_consolidado.csv"
)

# ============================================================================
# CSV
# ============================================================================

df = pd.read_csv(CSV_PATH)

print(f"\n✓ Registros carregados: {len(df):,}")

# ============================================================================
# FLINK ENV
# ============================================================================

env = StreamExecutionEnvironment.get_execution_environment()

env.set_parallelism(1)

settings = EnvironmentSettings.in_streaming_mode()

t_env = StreamTableEnvironment.create(
    env,
    environment_settings=settings
)

# ============================================================================
# EVENTOS
# ============================================================================

eventos = []

for _, row in df.iterrows():

    eventos.append(

        (
            str(row["AnoMes"]),
            str(row["NATUREZA"]),
            float(row["VALOR"]),
            int(row["QUANTIDADE"])
        )

    )

# ============================================================================
# DATASTREAM
# ============================================================================

stream = env.from_collection(
    collection=eventos,
    type_info=Types.ROW(
        [
            Types.STRING(),
            Types.STRING(),
            Types.FLOAT(),
            Types.INT()
        ]
    )
)

# ============================================================================
# PRINT STREAM
# ============================================================================

stream.print()

# ============================================================================
# EXECUTE
# ============================================================================

env.execute("pix-simple-stream")