import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")

from common.utils import normalizar_coluna, limpar_texto, converter_float

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

input_path = os.path.join(BASE_DIR, "common", "files", "03_Base_Impacto_Financeiro.csv")
output_path = os.path.join(BASE_DIR, "output", "silver", "03_Base_Impacto_Financeiro_Silver.csv")

df = pd.read_csv(input_path, sep=";")

# Padronizar colunas
df.columns = [normalizar_coluna(c) for c in df.columns]

# Limpar textos
df = df.convert_dtypes()
for col in df.select_dtypes(include="string"):
    df[col] = df[col].apply(limpar_texto)

# Converter valores financeiros
colunas_numericas = [
    "pf_original",
    "valor_por_pf",
    "custo_original",
    "economia_absoluta",
    "economia_percentual"
]

for col in colunas_numericas:
    if col in df.columns:
        df[col] = df[col].apply(converter_float)

# Remover duplicados
df = df.drop_duplicates(subset=["hu_id"])
df = df[df["hu_id"].notnull()]

df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")

print("Financeiro processado com sucesso!")
print(df.head(5).to_string(index=False))