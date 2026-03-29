import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings("ignore")

# =========================
# PATHS — ajuste conforme sua estrutura de pastas
# =========================
ENRICHED_DIR = r"C:\Users\VAL\Downloads\tratamento-de-dados\app\src\output\enriched"
GOLD_DIR     = r"C:\Users\VAL\Downloads\tratamento-de-dados\app\src\output\gold"

pf_path         = os.path.join(ENRICHED_DIR, "01_Base_Mensuracao_PF_Final.csv")
qualidade_path  = os.path.join(ENRICHED_DIR, "02_Base_Qualidade_Requisitos_Final.csv")
financeiro_path = os.path.join(ENRICHED_DIR, "03_Base_Impacto_Financeiro_Final.csv")
jira_path       = os.path.join(ENRICHED_DIR, "stories_JIRA_Final.csv")

os.makedirs(GOLD_DIR, exist_ok=True)
output_path = os.path.join(GOLD_DIR, "fato_historias_gold.csv")


# =========================
# 1. LEITURA
# =========================
pf        = pd.read_csv(pf_path,         sep=";")
qualidade = pd.read_csv(qualidade_path,  sep=";")
financeiro= pd.read_csv(financeiro_path, sep=";")
jira      = pd.read_csv(jira_path,       sep=";")

# Remover coluna lixo do JIRA
jira = jira.loc[:, ~jira.columns.str.startswith("Unnamed")]


# =========================
# 2. PADRONIZAÇÃO DE CHAVE
# =========================
# PF/Qualidade/Financeiro: hu_0001 ... hu_0120  (dados sintéticos)
# JIRA: 10000, 10001 ...                         (IDs reais do Jira)
# Estratégia: normalizar JIRA para hu_10000 etc. → sem colisão

for df in [pf, qualidade, financeiro]:
    df["hu_id"] = df["hu_id"].astype(str).str.strip()

jira["hu_id"] = "hu_" + jira["hu_id"].astype(str).str.strip().str.zfill(4)

# Marcar origem de cada registro
pf["origem"]        = "sintetico"
jira["origem"]      = "jira"


# =========================
# 3. RESOLVER CONFLITO DE COLUNA: score_qualidade
#    Existe em JIRA (score baseado em descrição) E em Qualidade (score de requisitos)
#    → renomear antes do join para não silenciar nenhuma
# =========================
qualidade = qualidade.rename(columns={"score_qualidade": "score_qualidade_requisitos",
                                       "nivel_qualidade": "nivel_qualidade_requisitos"})

jira = jira.rename(columns={"score_qualidade": "score_qualidade_jira"})


# =========================
# 4. CONSTRUIR BLOCO SINTÉTICO (PF + Qualidade + Financeiro)
# =========================
pf_cols = [
    "hu_id", "origem", "projeto", "sprint", "tipo",
    "pf_inicial", "pf_final", "desvio_pf", "desvio_percentual",
    "classificacao_pf", "flag_outlier",
    "custo_inicial", "custo_final", "economia"
]

qualidade_cols = [
    "hu_id",
    "score_qualidade_requisitos", "nivel_qualidade_requisitos",
    "num_palavras", "num_criterios_aceite", "num_regras_negocio",
    "completude", "tem_criterios", "tem_regras", "tem_tecnico",
    "flag_baixa_qualidade"
]

financeiro_cols = [
    "hu_id",
    "pf_original", "valor_por_pf", "custo_original",
    "custo_por_pf_real", "classificacao_financeira", "flag_custo_alto"
]

pf        = pf[[c for c in pf_cols        if c in pf.columns]]
qualidade = qualidade[[c for c in qualidade_cols  if c in qualidade.columns]]
financeiro= financeiro[[c for c in financeiro_cols if c in financeiro.columns]]

sintetico = (
    pf
    .merge(qualidade,  on="hu_id", how="left")
    .merge(financeiro, on="hu_id", how="left")
)


# =========================
# 5. CONSTRUIR BLOCO JIRA
# =========================
jira_cols = [
    "hu_id", "origem",
    "assignee", "data_entrega", "data_processamento", "data_valida",
    "lead_time_dias",
    "complexidade_textual", "qualidade_descricao", "qualidade_dado",
    "score_qualidade_jira", "confiabilidade", "descricao",
    "descricao_curta", "descricao_longa", "descricao_vazia",
    "qtd_palavras", "tamanho_descricao"
]

jira = jira[[c for c in jira_cols if c in jira.columns]]


# =========================
# 6. UNION (CONCAT) DOS DOIS BLOCOS
#    Colunas exclusivas de cada bloco ficam NaN no outro — comportamento esperado
# =========================
df = pd.concat([sintetico, jira], ignore_index=True, sort=False)


# =========================
# 7. GARANTIR TIPOS NUMÉRICOS
# =========================
numericas = [
    "pf_inicial", "pf_final", "desvio_pf", "desvio_percentual",
    "custo_inicial", "custo_final", "economia",
    "pf_original", "valor_por_pf", "custo_original", "custo_por_pf_real",
    "lead_time_dias",
    "score_qualidade_requisitos", "score_qualidade_jira", "confiabilidade",
    "num_palavras", "num_criterios_aceite", "num_regras_negocio", "completude",
    "qtd_palavras", "tamanho_descricao"
]

for col in numericas:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")


# =========================
# 8. FEATURES DE NEGÓCIO
# =========================

# Eficiência de custo por PF (apenas sintético)
df["eficiencia_custo_pf"] = np.where(
    df["pf_final"] > 0,
    df["custo_final"] / df["pf_final"],
    np.nan
)

# Performance de entrega
def classificar_performance(lead_time):
    if pd.isna(lead_time):
        return "Desconhecido"
    elif lead_time <= 5:
        return "Rápida"
    elif lead_time <= 15:
        return "Normal"
    else:
        return "Lenta"

df["performance_entrega"] = df["lead_time_dias"].apply(classificar_performance)

# Score de qualidade unificado
# Prioridade: score_qualidade_jira (se disponível), senão score_qualidade_requisitos
df["score_qualidade"] = df["score_qualidade_jira"].combine_first(df["score_qualidade_requisitos"])

# Risco geral
df["risco_geral"] = False

if "flag_outlier" in df.columns:
    df["risco_geral"] = df["risco_geral"] | (df["flag_outlier"].fillna(False) == True)

if "descricao_vazia" in df.columns:
    df["risco_geral"] = df["risco_geral"] | (df["descricao_vazia"].fillna(False) == True)

if "data_valida" in df.columns:
    df["risco_geral"] = df["risco_geral"] | (df["data_valida"].fillna(True) == False)

if "flag_baixa_qualidade" in df.columns:
    df["risco_geral"] = df["risco_geral"] | (df["flag_baixa_qualidade"].fillna(False) == True)

# Score global
def score_global(row):
    pontos, total = 0, 0

    sq = row.get("score_qualidade")
    if pd.notna(sq):
        total += 1
        if sq >= 0.8:
            pontos += 1

    conf = row.get("confiabilidade")
    if pd.notna(conf):
        total += 1
        if conf >= 0.8:
            pontos += 1

    fo = row.get("flag_outlier")
    if pd.notna(fo):
        total += 1
        if fo == False:
            pontos += 1

    return round(pontos / total, 4) if total > 0 else np.nan

df["score_global"] = df.apply(score_global, axis=1)


# =========================
# 9. REMOVER DUPLICADOS
# =========================
df = df.drop_duplicates(subset=["hu_id"])


# =========================
# 10. ORGANIZAÇÃO FINAL — WIDE TABLE PARA DASHBOARD
# =========================
colunas_finais = [
    # — Dimensões —
    "hu_id", "origem", "projeto", "sprint", "tipo", "assignee",
    "data_entrega", "data_processamento", "data_valida",

    # — Métricas de Pontos de Função —
    "pf_inicial", "pf_final", "desvio_pf", "desvio_percentual",
    "pf_original", "valor_por_pf",

    # — Métricas de Custo —
    "custo_inicial", "custo_final", "economia",
    "custo_original", "custo_por_pf_real",

    # — Métricas de Tempo —
    "lead_time_dias",

    # — Qualidade (Unificada) —
    "score_qualidade",
    "score_qualidade_requisitos", "nivel_qualidade_requisitos",
    "score_qualidade_jira",
    "num_palavras", "num_criterios_aceite", "num_regras_negocio",
    "completude", "tem_criterios", "tem_regras", "tem_tecnico",

    # — Qualidade JIRA —
    "complexidade_textual", "qualidade_descricao", "qualidade_dado",
    "confiabilidade", "qtd_palavras", "tamanho_descricao",

    # — Classificações —
    "classificacao_pf", "classificacao_financeira", "performance_entrega",

    # — Features calculadas —
    "eficiencia_custo_pf", "score_global",

    # — Flags e Riscos —
    "flag_outlier", "flag_custo_alto", "flag_baixa_qualidade", "descricao",
    "descricao_curta", "descricao_longa", "descricao_vazia",
    "risco_geral",
]

df = df[[c for c in colunas_finais if c in df.columns]]


# =========================
# 11. SALVAR
# =========================
df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")

# =========================
# LOG
# =========================
print("=" * 55)
print("  BASE GOLD — fato_historias  ")
print("=" * 55)
print(f"\nTotal de registros : {len(df)}")
print(f"  → Sintéticos (PF): {(df['origem'] == 'sintetico').sum()}")
print(f"  → JIRA real       : {(df['origem'] == 'jira').sum()}")
print(f"\nColunas            : {len(df.columns)}")
print(f"Arquivo gerado     : {output_path}")
print()
print("--- Completude por origem ---")
for origem in df["origem"].dropna().unique():
    sub = df[df["origem"] == origem]
    completude = (sub.notna().sum() / len(sub) * 100).mean()
    print(f"  {origem:12s}: {completude:.1f}% campos preenchidos")

print()
print("--- Distribuição de risco ---")
print(df["risco_geral"].value_counts().to_string())
print()
print("--- Performance de entrega ---")
print(df["performance_entrega"].value_counts().to_string())
print()
print("--- Amostra (primeiras 3 linhas) ---")
print(df[["hu_id", "origem", "projeto", "pf_final", "score_qualidade", "score_global", "risco_geral"]].head(3).to_string(index=False))