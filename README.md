# Tratamento de Dados - Pipeline_PI

Pipeline de ETL em Python para ingestão, transformação e geração de arquivos `silver` e `final` a partir de três bases de dados:
- `PF` (01_Base_Mensuracao_PF)
- `Qualidade` (02_Base_Qualidade_Requisitos)
- `Financeiro` (03_Base_Impacto_Financeiro)

Arquitetura modular em `app/src` com camadas `common`, `enriched` e `silver`.

---

## 🚀 Estrutura do repositório

```
app/src/
  common/
    utils.py
    files/
      01_Base_Mensuracao_PF.csv
      02_Base_Qualidade_Requisitos.csv
      03_Base_Impacto_Financeiro.csv
  enriched/
    __init__.py
    dim_tipo_funcao.py
    enrich_financeiro.py
    enrich_pf.py
    enrich_qualidade.py
  silver/
    __init__.py
    process_financeiro.py
    process_pf.py
    process_qualidade.py
  output/
    dim_tipo_funcao_pf.csv
    enriched/
      01_Base_Mensuracao_PF_Final.csv
      02_Base_Qualidade_Requisitos_Final.csv
      03_Base_Impacto_Financeiro_Final.csv
    silver/
      01_Base_Mensuracao_PF_Silver.csv
      02_Base_Qualidade_Requisitos_Silver.csv
      03_Base_Impacto_Financeiro_Silver.csv
```

---

## 🧩 Visão geral de cada camada

- `common/utils.py`: funções utilitárias de leitura, validação e gravação de dados.
- `enriched/`: implementa lógica de negócio e enriquece cada base.
  - `dim_tipo_funcao.py`: gera dimensão de tipo de função para PF.
  - `enrich_*.py`: aplica transformações específicas por domínio.
- `silver/`: transforma e persiste os dados em nível `silver`.
- `output/`: guarda arquivos finais (`enriched` + `silver`) e dimensões consolidadas.

---

## ▶️ Como executar

No terminal, na raiz do projeto:

```powershell
cd app/src
python -m enriched.enrich_pf
python -m enriched.enrich_qualidade
python -m enriched.enrich_financeiro
python -m silver.process_pf
python -m silver.process_qualidade
python -m silver.process_financeiro
```

> Observação: execute as etapas de `enriched` antes das etapas de `silver`.

---

## 🧪 Resultado esperado

- Geração de arquivos `silver` em `app/src/output/silver`
- Geração de arquivos `final/enriched` em `app/src/output/enriched`
- Geração de dimensão em `app/src/output/dim_tipo_funcao_pf.csv`

Fluxo recomendado:
1. `common/files/*` → `enriched`
2. `enriched` → `silver`
3. salvar produto final em `output/enriched`

---

## 🛠️ Requisitos

- Python 3.10+
- pacotes: `pandas`, `numpy`

Instalação (caso exista `requirements.txt`):

```powershell
pip install -r requirements.txt
```

Ou manual:

```powershell
pip install pandas numpy
```

---

## 📌 Boas práticas e próximos passos

- Criar `requirements.txt` e ambiente virtual (`venv`/`conda`).
- Adicionar testes unitários (`pytest`).
- Configurar CI (GitHub Actions) para pipeline automatizado.
- Implementar logs e tratamento de exceções.
- Documentar DDL/metadata (campos, tipos, regras de negócio).

---

## 🔏 Observações

- Este README está pronto para uso em repositório público ou privado.
- Atualize sempre que for adicionada nova camada (por exemplo, `app/src/gold/`, integração com Databricks, etc.).
