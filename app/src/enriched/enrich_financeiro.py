import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

input_path = os.path.join(BASE_DIR, "output", "silver", "03_Base_Impacto_Financeiro_Silver.csv")
output_path = os.path.join(BASE_DIR, "output", "enriched", "03_Base_Impacto_Financeiro_Final.csv")

df = pd.read_csv(input_path, sep=";")

# =========================
# 0. PADRONIZAR NOMES (CRÍTICO)
# =========================
df = df.rename(columns={
    "valor_por_pf_r$": "valor_por_pf",
    "custo_original_r$": "custo_original"
})

# =========================
# 1. TIPAGEM
# =========================
colunas_numericas = [
    "pf_original",
    "valor_por_pf",
    "custo_original"
]

for col in colunas_numericas:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# =========================
# 2. VALIDAÇÃO
# =========================
df = df[df["pf_original"].notna()]
df = df[df["pf_original"] > 0]

df = df[df["custo_original"].notna()]
df = df[df["custo_original"] > 0]

# =========================
# 3. MÉTRICAS
# =========================
df["custo_por_pf_real"] = df["custo_original"] / df["pf_original"]

# =========================
# 4. CLASSIFICAÇÃO SIMPLES (já que não tem economia ainda)
# =========================
def classificar(row):
    if pd.isna(row["custo_por_pf_real"]):
        return "Sem dados"
    elif row["custo_por_pf_real"] <= row["valor_por_pf"]:
        return "Eficiente"
    else:
        return "Acima do esperado"

df["classificacao_financeira"] = df.apply(classificar, axis=1)

# =========================
# 5. FLAGS
# =========================
df["flag_custo_alto"] = df["custo_por_pf_real"] > df["valor_por_pf"]

# =========================
# 6. ORGANIZAÇÃO
# =========================
colunas_ordenadas = [
    "hu_id",
    "pf_original",
    "valor_por_pf",
    "custo_original",
    "custo_por_pf_real",
    "classificacao_financeira",
    "flag_custo_alto"
]

df = df[[col for col in colunas_ordenadas if col in df.columns]]


# =========================
# 7. SALVAR
# =========================
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")

print("Base Financeira enriquecida com sucesso!")
print(df.head().to_string(index=False))