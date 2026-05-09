import os
import json
import requests

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

BASE_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/"
    "Pix_DadosAbertos/versao/v1/odata/"
    "EstatisticasTransacoesPix(Database=@Database)"
)

BASES = ["202501"]

HEADERS = {
    "Accept": "application/json"
}

# ============================================================================
# PASTAS
# ============================================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

os.makedirs(RAW_DIR, exist_ok=True)

# ============================================================================
# DOWNLOAD
# ============================================================================

print("\n" + "=" * 60)
print(" PIX DOWNLOADER — BANCO CENTRAL")
print("=" * 60)

for database in BASES:

    print(f"\n[INFO] Baixando base {database}...")

    params = {
        "@Database": f"'{database}'",
        "$top": 100000,
        "$format": "json"
    }

    try:

        response = requests.get(
            BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=60
        )

        print(f"  STATUS: {response.status_code}")

        response.raise_for_status()

        data = response.json()

        output_path = os.path.join(
            RAW_DIR,
            f"pix_{database}.json"
        )

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                data,
                f,
                ensure_ascii=False,
                indent=2
            )

        qtd = len(data.get("value", []))

        print(f"  ✓ Registros: {qtd}")
        print(f"  ✓ Salvo em: {output_path}")

    except Exception as e:

        print(f"  ❌ Erro ao baixar {database}")
        print(f"  → {e}")

print("\n✅ Download concluído.")