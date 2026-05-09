import os
import pandas as pd
import matplotlib.pyplot as plt

from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment
)

# ============================================================================
# CAMINHOS
# ============================================================================

BASE_DIR = os.path.dirname(
    
        os.path.dirname(os.path.abspath(__file__))
    
)

CSV_PATH = os.path.join(
    BASE_DIR,
    "data",
    "processed",
    "pix_consolidado.csv"
)

OUTPUTS_DIR = os.path.join(
    BASE_DIR,
    "outputs"
)

CHARTS_DIR = os.path.join(
    OUTPUTS_DIR,
    "charts"
)

os.makedirs(CHARTS_DIR, exist_ok=True)

# ============================================================================
# VERIFICAÇÃO
# ============================================================================

if not os.path.exists(CSV_PATH):

    raise Exception(
        f"CSV não encontrado:\n{CSV_PATH}"
    )

# ============================================================================
# LEITURA COM PANDAS
# ============================================================================

print("\n" + "=" * 60)
print(" PIX ANALYTICS — APACHE FLINK")
print("=" * 60)

print("\n[INFO] Carregando CSV consolidado...")

df = pd.read_csv(CSV_PATH)
print("\nMeses encontrados:")

print(sorted(df["AnoMes"].unique()))

print(f"✓ Registros carregados: {len(df):,}")

# ============================================================================
# PYFLINK
# ============================================================================

print("\n[INFO] Inicializando PyFlink...")

settings = EnvironmentSettings.in_batch_mode()

t_env = TableEnvironment.create(settings)

# ============================================================================
# DATAFRAME → FLINK TABLE
# ============================================================================

print("[INFO] Convertendo DataFrame para Flink Table...")

table = t_env.from_pandas(df)

t_env.create_temporary_view(
    "pix",
    table
)

print("✓ Tabela registrada: pix")

# ============================================================================
# ANÁLISE 1 — EVOLUÇÃO TEMPORAL
# ============================================================================

print("\n" + "=" * 60)
print(" ANÁLISE 1 — EVOLUÇÃO TEMPORAL")
print("=" * 60)

resultado = t_env.execute_sql("""

    SELECT
        AnoMes,
        SUM(VALOR)       AS ValorTotal,
        SUM(QUANTIDADE)  AS QuantidadeTotal
    FROM pix
    GROUP BY AnoMes
    ORDER BY AnoMes

""")

print(f"\n{'AnoMes':<12} {'Valor Total':>20} {'Quantidade':>20}")

print("-" * 56)
dados_temporais = []

for row in resultado.collect():

    ano_mes = str(row[0])

    valor = float(row[1]) if row[1] else 0

    qtd = int(row[2]) if row[2] else 0

    dados_temporais.append(
        {
            "AnoMes": ano_mes,
            "ValorTotal": valor,
            "QuantidadeTotal": qtd
        }
    )

    print(
        f"{ano_mes:<12} "
        f"{valor:>20,.2f} "
        f"{qtd:>20,}"
    )

# ============================================================================
# DATAFRAME — EVOLUÇÃO TEMPORAL
# ============================================================================

df_temporal = pd.DataFrame(dados_temporais)

print("\n✓ DataFrame temporal criado.")

# ============================================================================
# GRÁFICO — EVOLUÇÃO DO PIX
# ============================================================================

plt.figure(figsize=(12, 6))

plt.plot(
    df_temporal["AnoMes"],
    df_temporal["ValorTotal"]
)

plt.title("Evolução do Volume Financeiro do Pix")

plt.xlabel("Ano/Mês")

plt.ylabel("Valor Total")

plt.xticks(rotation=45)

plt.tight_layout()

grafico_path = os.path.join(
    CHARTS_DIR,
    "evolucao_pix.png"
)

plt.savefig(grafico_path)

print("\n✓ Gráfico salvo em:")

print(grafico_path)

# ============================================================================
# ANÁLISE 2.1 — FLUXO DAS TRANSAÇÕES POR NATUREZA
# ============================================================================

print("\n" + "=" * 60)
print(" ANÁLISE — EVOLUÇÃO TEMPORAL POR NATUREZA")
print("=" * 60)

resultado = t_env.execute_sql("""

    SELECT
        AnoMes,
        NATUREZA,
        SUM(VALOR) AS ValorTotal
    FROM pix
    GROUP BY AnoMes, NATUREZA
    ORDER BY AnoMes, NATUREZA

""")

dados_natureza = []

print(
    f"\n{'AnoMes':<12} "
    f"{'Natureza':<20} "
    f"{'Valor Total':>20}"
)

print("-" * 56)

for row in resultado.collect():

    ano_mes = str(row[0])

    natureza = str(row[1])

    valor = float(row[2]) if row[2] else 0

    dados_natureza.append(
        {
            "AnoMes": ano_mes,
            "NATUREZA": natureza,
            "ValorTotal": valor
        }
    )

    print(
        f"{ano_mes:<12} "
        f"{natureza:<20} "
        f"{valor:>20,.2f}"
    )

# ============================================================================
# DATAFRAME — NATUREZA
# ============================================================================

df_natureza = pd.DataFrame(dados_natureza)

print("\n✓ DataFrame por natureza criado.")

# ============================================================================
# GRÁFICO — NATUREZA AO LONGO DO TEMPO
# ============================================================================

plt.figure(figsize=(14, 7))

# Uma linha para cada natureza
for natureza in df_natureza["NATUREZA"].unique():

    subset = df_natureza[
        df_natureza["NATUREZA"] == natureza
    ]

    plt.plot(
        subset["AnoMes"],
        subset["ValorTotal"],
        label=natureza
    )

plt.title("Evolução Financeira do Pix por Natureza")

plt.xlabel("Ano/Mês")

plt.ylabel("Valor Total")

plt.xticks(rotation=45)

plt.legend()

plt.tight_layout()

grafico_natureza = os.path.join(
    CHARTS_DIR,
    "natureza_temporal.png"
)

plt.savefig(grafico_natureza)

print("\n✓ Gráfico por natureza salvo em:")

print(grafico_natureza)

# ============================================================================
# ANÁLISE 2.2 — TOTAL POR NATUREZA
# ============================================================================

print("\n" + "=" * 60)
print(" ANÁLISE 2 — RANKING POR NATUREZA")
print("=" * 60)

resultado = t_env.execute_sql("""

    SELECT
        NATUREZA,
        SUM(VALOR) AS TotalValor,
        SUM(QUANTIDADE) AS TotalQuantidade
    FROM pix
    GROUP BY NATUREZA
    ORDER BY TotalValor DESC

""")

print(
    f"\n{'Natureza':<20} "
    f"{'Valor Total':>20} "
    f"{'Quantidade':>20}"
)

print("-" * 64)

for row in resultado.collect():

    natureza = str(row[0])

    valor = float(row[1]) if row[1] else 0

    qtd = int(row[2]) if row[2] else 0

    print(
        f"{natureza:<20} "
        f"{valor:>20,.2f} "
        f"{qtd:>20,}"
    )

# ============================================================================
# ANÁLISE 3 — VOLUME POR REGIÃO
# ============================================================================

print("\n" + "=" * 60)
print(" ANÁLISE 3 — VOLUME POR REGIÃO")
print("=" * 60)

resultado = t_env.execute_sql("""

    SELECT
        PAG_REGIAO,
        SUM(VALOR) AS TotalValor,
        SUM(QUANTIDADE) AS TotalQuantidade
    FROM pix
    GROUP BY PAG_REGIAO
    ORDER BY TotalValor DESC

""")

print(
    f"\n{'Região':<20} "
    f"{'Valor Total':>20} "
    f"{'Quantidade':>20}"
)

print("-" * 64)
dados_regiao = []

for row in resultado.collect():

    regiao = str(row[0])

    valor = float(row[1]) if row[1] else 0

    qtd = int(row[2]) if row[2] else 0

    dados_regiao.append(
        {
            "REGIAO": regiao,
            "ValorTotal": valor,
            "QuantidadeTotal": qtd
        }
    )

    print(
        f"{regiao:<20} "
        f"{valor:>20,.2f} "
        f"{qtd:>20,}"
    )

# ============================================================================
# DATAFRAME — REGIÕES
# ============================================================================

df_regiao = pd.DataFrame(dados_regiao)

print("\n✓ DataFrame regional criado.")

# ============================================================================
# GRÁFICO — REGIÕES
# ============================================================================

plt.figure(figsize=(10, 6))

plt.bar(
    df_regiao["REGIAO"],
    df_regiao["ValorTotal"]
)

plt.title("Volume Financeiro do Pix por Região")

plt.xlabel("Região")

plt.ylabel("Valor Total")

plt.xticks(rotation=15)

plt.tight_layout()

grafico_regiao = os.path.join(
    CHARTS_DIR,
    "regioes_pix.png"
)

plt.savefig(grafico_regiao)

print("\n✓ Gráfico regional salvo em:")

print(grafico_regiao)

# ============================================================================
# FINAL
# ============================================================================

print("\n" + "=" * 60)
print(" JOB FINALIZADO COM SUCESSO")
print("=" * 60)