import os
import json
import pandas as pd

# ============================================================================
# PASTAS
# ============================================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

os.makedirs(PROCESSED_DIR, exist_ok=True)

OUTPUT_CSV = os.path.join(
    PROCESSED_DIR,
    "pix_consolidado.csv"
)

# ============================================================================
# LEITURA DOS JSONS
# ============================================================================

print("\n" + "=" * 60)
print(" NORMALIZAÇÃO DOS DADOS PIX")
print("=" * 60)

arquivos = sorted([
    f for f in os.listdir(RAW_DIR)
    if f.endswith(".json")
])

if not arquivos:
    raise Exception(
        "Nenhum JSON encontrado em data/raw/"
    )

dataframes = []

# ============================================================================
# PROCESSAMENTO
# ============================================================================

for arquivo in arquivos:

    caminho = os.path.join(RAW_DIR, arquivo)

    print(f"\n[INFO] Lendo: {arquivo}")

    with open(caminho, "r", encoding="utf-8") as f:
        data = json.load(f)

    registros = data.get("value", [])

    if not registros:
        print("  ⚠ Nenhum registro encontrado.")
        continue

    df = pd.DataFrame(registros)

    print(f"  ✓ Registros: {len(df)}")

    dataframes.append(df)

# ============================================================================
# CONCATENAÇÃO
# ============================================================================

print("\n[INFO] Consolidando datasets...")

df_final = pd.concat(
    dataframes,
    ignore_index=True
)

# ============================================================================
# LIMPEZA BÁSICA
# ============================================================================

print("[INFO] Aplicando limpeza básica...")

# Remove espaços extras em colunas texto
colunas_texto = [
    "PAG_PFPJ",
    "REC_PFPJ",
    "PAG_REGIAO",
    "REC_REGIAO",
    "PAG_IDADE",
    "REC_IDADE",
    "FORMAINICIACAO",
    "NATUREZA",
    "FINALIDADE"
]

for coluna in colunas_texto:

    if coluna in df_final.columns:

        df_final[coluna] = (
            df_final[coluna]
            .astype(str)
            .str.strip()
        )

# Remove duplicados
df_final = df_final.drop_duplicates()

# Ordena temporalmente
df_final = df_final.sort_values(
    by="AnoMes"
)

# ============================================================================
# EXPORTAÇÃO
# ============================================================================

df_final.to_csv(
    OUTPUT_CSV,
    index=False,
    encoding="utf-8"
)

# ============================================================================
# RESUMO
# ============================================================================

print("\n" + "=" * 60)
print(" RESUMO")
print("=" * 60)

print(f"Total de registros: {len(df_final):,}")

print(f"Colunas:")
for coluna in df_final.columns:
    print(f"  - {coluna}")

print(f"\n✓ CSV consolidado salvo em:")
print(f"  {OUTPUT_CSV}")

print("\n✅ Normalização concluída.")