# AWS Data Engineering Learning Lab

Laboratório pequeno e real para estudar fundamentos de Engenharia de Dados na AWS. Ele não é uma plataforma de produção e não tenta parecer uma.

> Para este volume, Python/Pandas seria provavelmente mais simples e suficiente. PySpark está sendo usado para aprender o modelo de processamento distribuído utilizado em cargas maiores e pelo AWS Glue.

## Arquitetura

```text
Open-Meteo Historical Weather API
        ↓ Python
S3 / raw (JSON original envelopado)
        ↓ AWS Glue + PySpark
S3 / curated (Parquet particionado por data)
        ↓ Glue Data Catalog
Athena SQL
```

O fluxo usa duas camadas porque elas bastam para o objetivo: `raw` preserva a resposta da fonte e `curated` contém dados tipados, deduplicados e prontos para análise. Não existe uma camada `gold` artificial.

## Dataset

Escolha: [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), com dados horários de Belo Horizonte, São Paulo, Rio de Janeiro e Blumenau.

| Alternativa | Vantagem | Limitação para este lab | Decisão |
|---|---|---|---|
| Open-Meteo | API sem chave, tempo + local + métricas | O volume é pequeno | Escolhida: melhor equilíbrio didático |
| Agregados IBGE | Fonte brasileira e dimensões ricas | Estrutura da API e séries variam por pesquisa | Boa alternativa para outro lab |
| NYC TLC | Arquivos Parquet e volume real maior | Download mais pesado e sem ingestão REST simples | Desnecessário aqui |

Granularidade da fato: uma observação por `location_id + observed_at`. `dim_location` é o lookup geográfico.

## Como executar

### Local no ChatGPT Work/ambiente cloud

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
./scripts/run_local.sh
pytest -q
spark-submit glue/spark_experiments.py
```

### AWS CloudShell

1. No CloudShell autenticado, descompacte o projeto e gere um plano seguro. O script instala o Terraform 1.16.2 somente dentro do ambiente cloud, verifica o download oficial e não cria recursos:

```bash
tar -xzf aws-data-engineering-learning-lab.tar.gz
cd aws-data-engineering-learning-lab
./scripts/cloudshell_prepare.sh us-east-1
```

2. Revise o resumo do plano. Somente após aprovar o custo estimado de centavos de dólar, execute todo o pipeline, Catálogo e consultas Athena com um comando:

```bash
CONFIRM_AWS_COSTS=YES ./scripts/cloudshell_execute.sh us-east-1
```

O executor grava evidências em `evidence/`, gera um pacote final e mantém os recursos sem compute permanente para exploração posterior. Quando o estudo acabar, revise o alvo e execute `terraform -chdir=terraform destroy`.

3. Consulte o histórico no Athena e compare **Data scanned** entre as duas primeiras consultas de `sql/02_learning_queries.sql`. O executor já cria o catálogo e roda as consultas; a execução manual serve para exploração.
4. Quando terminar e quiser apagar o laboratório, execute `terraform -chdir=terraform destroy`. O bucket é exclusivo do lab e `force_destroy = true`; revise o alvo antes de confirmar, pois os objetos serão removidos.

O Terraform cria somente S3, Data Catalog database, workgroup Athena com limite de 100 MiB por consulta, IAM mínimo e um Glue Job de dois workers. Não cria Redshift, EMR, RDS, MWAA, NAT Gateway ou servidores permanentes.

## Stack

Python 3.11+, PySpark 3.5, AWS Glue 5.0, S3, Glue Data Catalog, Athena, Terraform, pytest e GitHub Actions. O DAG Airflow é validável como código, mas Airflow não é provisionado.

## Resultado validado

- execução real concluída em `us-east-1`: Terraform criou 9 recursos e o Glue terminou com `SUCCEEDED`;
- janeiro de 2025 gerou 2.976 observações horárias para quatro cidades, sem duplicidade;
- fato Parquet particionada por `year/month/day` e dimensão de localização consultadas no Athena;
- scan de todo o período: 13.426 bytes; scan de um dia: 457 bytes, redução de 96,6%;
- planos Spark locais mostraram lazy evaluation, `Exchange` (shuffle), broadcast join, skew e small files;
- 8 testes automatizados passaram.

## Comece aqui para estudar

Siga o [`docs/STUDY_GUIDE.md`](docs/STUDY_GUIDE.md): são oito sessões práticas, com arquivos para abrir, comandos, checkpoints e respostas de entrevista. A versão diagramada está em [`docs/AWS_Data_Engineering_Study_Guide.pdf`](docs/AWS_Data_Engineering_Study_Guide.pdf). Use [`docs/LEARNING.md`](docs/LEARNING.md) como referência conceitual detalhada.

A evidência resumida da execução está em [`evidence/aws_run_summary.json`](evidence/aws_run_summary.json) e o relato em [`docs/BUILD_LOG.md`](docs/BUILD_LOG.md).