import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

input_path = os.path.join(BASE_DIR, "output", "silver", "02_Base_Qualidade_Requisitos_Silver.csv")
output_path = os.path.join(BASE_DIR, "output", "enriched", "02_Base_Qualidade_Requisitos_Final.csv")

df = pd.read_csv(input_path, sep=";")

# =========================
# 1. TIPAGEM
# =========================
colunas_numericas = [
    "num_palavras",
    "num_criterios_aceite",
    "num_regras_negocio",
    "estrutura_tecnica_0/1",
    "clareza_score_0_10"
]

for col in colunas_numericas:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# =========================
# 2. SCORE DE QUALIDADE (ADAPTADO)
# =========================
df["score_qualidade"] = df[[
    "num_criterios_aceite",
    "num_regras_negocio",
    "clareza_score_0_10"
]].mean(axis=1)

# =========================
# 3. CLASSIFICAÇÃO
# =========================
def classificar(score):
    if pd.isna(score):
        return "Sem dados"
    elif score >= 7:
        return "Alta"
    elif score >= 4:
        return "Média"
    else:
        return "Baixa"

df["nivel_qualidade"] = df["score_qualidade"].apply(classificar)

# =========================
# 4. FEATURE PARA IA
# =========================
df["flag_baixa_qualidade"] = df["nivel_qualidade"] == "Baixa"

# =========================
# 5. FEATURES ESTRUTURAIS (MUITO FORTE)
# =========================
df["tem_criterios"] = df["num_criterios_aceite"] > 0
df["tem_regras"] = df["num_regras_negocio"] > 0
df["tem_tecnico"] = df["estrutura_tecnica_0/1"] == 1

# =========================
# 6. COMPLETUDE
# =========================
df["completude"] = df[colunas_numericas].notna().sum(axis=1)

# =========================
# 7. ORGANIZAÇÃO
# =========================
colunas_ordenadas = [
    "hu_id",
    "num_palavras",
    "num_criterios_aceite",
    "num_regras_negocio",
    "estrutura_tecnica_0/1",
    "clareza_score_0_10",
    "score_qualidade",
    "nivel_qualidade",
    "flag_baixa_qualidade",
    "completude",
    "tem_criterios",
    "tem_regras",
    "tem_tecnico"
]

df = df[[col for col in colunas_ordenadas if col in df.columns]]

# =========================
# 8. SALVAR
# =========================
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")

print("Base de Qualidade enriquecida com sucesso!")
print(df.head().to_string(index=False))