import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")

from common.utils import normalizar_coluna, limpar_texto, converter_float

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

input_path = os.path.join(BASE_DIR, "common", "files", "01_Base_Mensuracao_PF.csv")
output_path = os.path.join(BASE_DIR, "output/silver", "01_Base_Mensuracao_PF_Silver.csv")

df = pd.read_csv(input_path, sep=";")



# =========================
# 1. PADRONIZAR COLUNAS
# =========================
df.columns = [normalizar_coluna(c) for c in df.columns]




# =========================
# 2. VALIDAR COLUNAS
# =========================
colunas_esperadas = [
    "hu_id", "tipo", "pf_inicial", "pf_final",
    "valor_por_pf", "custo_inicial", "custo_final"
]

for col in colunas_esperadas:
    if col not in df.columns:
        raise ValueError(f"Coluna obrigatória ausente: {col}")



# =========================
# 3. LIMPEZA DE TEXTO
# =========================
df = df.convert_dtypes()

for col in df.select_dtypes(include="string"):
    df[col] = df[col].apply(limpar_texto)

df["hu_id"] = df["hu_id"].astype(str).str.strip()


# =========================
# CRIAR COLUNA OPERACAO
# =========================
def map_operacao(tipo):
    tipo = str(tipo).lower()
    
    if "nova funcionalidade" in tipo:
        return "inclusao"
    elif "evolutiva" in tipo:
        return "alteracao"
    elif "corretiva" in tipo:
        return "alteracao"
    
    return None

df["operacao"] = df["tipo"].apply(map_operacao)


# =========================
# 4. CONVERSÃO NUMÉRICA
# =========================
colunas_numericas = [
    "pf_inicial", "pf_final",
    "valor_por_pf", "custo_inicial", "custo_final"
]

for col in colunas_numericas:
    df[col] = df[col].apply(converter_float).fillna(0)


# =========================
# 5. REMOVER DUPLICADOS
# =========================
df = df.drop_duplicates(subset=["hu_id"])
df = df[df["hu_id"].notnull()]


# =========================
# 6. SALVAR
# =========================
df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")

print("PF processado com sucesso!")
print(df.head(5).to_string(index=False))