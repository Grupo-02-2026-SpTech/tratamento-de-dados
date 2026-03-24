import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

pf_path = os.path.join(BASE_DIR, "output/silver", "01_Base_Mensuracao_PF_Silver.csv")
output_path = os.path.join(BASE_DIR, "output", "enriched", "01_Base_Mensuracao_PF_Final.csv")

df = pd.read_csv(pf_path, sep=";")

# =========================
# 1. PADRONIZAR CAMPOS
# =========================
df["tipo"] = df["tipo"].astype("string").str.strip().str.lower()

# =========================
# 2. TRATAMENTO DE DADOS
# =========================

# Garantir tipos numéricos
df["pf_inicial"] = pd.to_numeric(df["pf_inicial"], errors="coerce")
df["pf_final"] = pd.to_numeric(df["pf_final"], errors="coerce")
df["custo_inicial"] = pd.to_numeric(df["custo_inicial"], errors="coerce")
df["custo_final"] = pd.to_numeric(df["custo_final"], errors="coerce")

# Remover registros críticos inválidos
df = df[df["pf_inicial"].notna()]
df = df[df["pf_final"].notna()]
df = df[df["pf_inicial"] > 0]

# Remover valores absurdos (proteção básica)
df = df[df["pf_inicial"] < 1000]
df = df[df["pf_final"] < 1000]

# =========================
# 3. DESVIO REAL
# =========================
df["desvio_pf"] = df["pf_final"] - df["pf_inicial"]

df["desvio_percentual"] = df["desvio_pf"] / df["pf_inicial"]

# =========================
# 4. CLASSIFICAÇÃO
# =========================
def classificar(row):
    if pd.isna(row["desvio_pf"]):
        return "Sem dados"
    
    if abs(row["desvio_pf"]) <= 2:
        return "Estável"
    elif row["desvio_pf"] < 0:
        return "Otimizado"
    else:
        return "Aumentou"

df["classificacao_pf"] = df.apply(classificar, axis=1)

# =========================
# 5. FEATURE PARA IA
# =========================
df["flag_outlier"] = df["classificacao_pf"] == "Aumentou"

# =========================
# 6. INDICADOR FINANCEIRO
# =========================
df["economia"] = df["custo_inicial"] - df["custo_final"]

# =========================
# 7. ORGANIZAÇÃO FINAL (opcional, mas melhora leitura)
# =========================
colunas_ordenadas = [
    "hu_id", "projeto", "sprint", "tipo",
    "pf_inicial", "pf_final", "desvio_pf", "desvio_percentual",
    "classificacao_pf", "flag_outlier",
    "custo_inicial", "custo_final", "economia"
]

df = df[[col for col in colunas_ordenadas if col in df.columns]]


# =========================
# 8. SALVAR
# =========================
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")

print("Base PF enriquecida com sucesso!")
print(df.head().to_string(index=False))