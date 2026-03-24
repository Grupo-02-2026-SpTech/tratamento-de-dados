import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")

from common.utils import normalizar_coluna, limpar_texto, converter_float

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

input_path = os.path.join(BASE_DIR, "common", "files", "02_Base_Qualidade_Requisitos.csv")
output_path = os.path.join(BASE_DIR, "output", "silver", "02_Base_Qualidade_Requisitos_Silver.csv")

df = pd.read_csv(input_path, sep=";")

# =========================
# 1. PADRONIZAR COLUNAS
# =========================
df.columns = [normalizar_coluna(c) for c in df.columns]

# =========================
# 2. LIMPEZA DE TEXTO
# =========================
df = df.convert_dtypes()

for col in df.select_dtypes(include="string"):
    df[col] = df[col].apply(limpar_texto)

# =========================
# 3. TRATAMENTO NUMÉRICO
# =========================
colunas_numericas = [
    "clareza",
    "criterios_aceite",
    "detalhamento_tecnico",
    "objetividade",
    "score_qualidade"
]

for col in colunas_numericas:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col].apply(converter_float), errors="coerce")

# =========================
# 4. REMOVER DADOS RUINS
# =========================
df = df.drop_duplicates(subset=["hu_id"])
df = df[df["hu_id"].notna()]

# =========================
# 5. SALVAR
# =========================
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")

print("Qualidade processada com sucesso!")
print(df.head(5).to_string(index=False))