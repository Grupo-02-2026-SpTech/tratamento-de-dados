import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

output_path = os.path.join(BASE_DIR, "output", "dim_tipo_funcao_pf.csv")

dados = [
    ["Dados Negócio/Integração", 7.00, 5.60, 2.10],
    ["Funcionalidade", 4.60, 3.68, 1.38],
    ["Componente / Microsserviço", 1.10, 0.88, 0.33],
    ["Dado de Código", 4.60, 3.68, 1.38],
    ["DEV compartilhado", 2.30, 1.84, 0.69],
    ["Expurgo de Dados", 4.60, 4.60, 4.60],
    ["Feature Flag (liga/desliga)", 4.60, 3.68, 1.38],
    ["Funcionalidades apenas Testadas", 1.15, 1.15, 1.15],
    ["Merge", 0.80, 0.64, 0.24],
    ["Operações Lógicas e Matemáticas", 4.60, 3.68, 1.38],
    ["Parametrização / Configuração", 1.20, 1.20, 1.20],
    ["Plataforma Adicional", 4.60, 3.68, 1.38],
    ["Service Enablement", 1.15, 0.92, 0.35],
    ["UX - User Experience", 1.15, 1.15, 1.15],
    ["Outro", 1.00, 0.80, 0.30],
]

df = pd.DataFrame(dados, columns=[
    "tipo_funcao", "pf_inclusao", "pf_alteracao", "pf_exclusao"
])

df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")

print("Dimensão de tipo de função criada!")