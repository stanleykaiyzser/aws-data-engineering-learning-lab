# Learning Guide

Este arquivo é o mapa de estudo do laboratório. A regra é observar o código e a execução antes de decorar definições.

## Evidências da validação local — 9 de setembro de 2026

- Fonte real: quatro respostas da Open-Meteo para 1–7 de janeiro de 2025.
- RAW: 4 JSONs, aproximadamente 92 KiB.
- CURATED: 672 linhas, 0 inválidas, 0 duplicadas, 7 partitions diárias e 1 arquivo de dimensão.
- O Parquet particionado ocupou aproximadamente 144 KiB: neste volume minúsculo, metadados e sete arquivos superam o ganho de compressão. É uma evidência contra extrapolar benchmarks pequenos.
- Lazy action `count`: 1,83 s na execução observada.
- Partitions: 4 iniciais → 8 com `repartition` → 2 com `coalesce`.
- Shuffle: `Exchange` apareceu no plano de `groupBy`.
- Broadcast: `BroadcastExchange` e `BroadcastHashJoin` apareceram no plano.
- Skew: a chave `hot` concentrou 18.000 de 20.000 linhas (90%).
- Small files: leitura de 24 arquivos levou 0,56 s; leitura de 2 arquivos, 0,22 s nessa execução local. É observação didática, não benchmark universal.
- Testes: 8 passaram, incluindo rerun idempotente, contrato S3, separação do SQL Athena, Lambda, deduplicação PySpark e falha controlada de qualidade.

## O que foi implementado e o que ficou conceitual

| Tema | Evidência | Status |
|---|---|---|
| RAW → CURATED | `src/ingest.py` e `glue/weather_transform.py` | Implementado |
| Glue/PySpark + Parquet | job de transformação + `evidence/aws_run_summary.json` | Implementado e executado na AWS |
| Catálogo + Athena | Terraform + `sql/` + evidência resumida | Implementado e executado na AWS |
| Lazy, partitions, shuffle, broadcast, skew, small files | `glue/spark_experiments.py` | Experimentos executáveis |
| Modelagem dimensional | `fact_weather_hourly` + `dim_location` | Implementado |
| Data Quality, falhas e idempotência | job + testes | Implementado |
| Lambda | validador mínimo de evento RAW | Código implementado; deploy deliberadamente opcional |
| Airflow | DAG de referência | Código validado; infraestrutura não provisionada |
| Terraform e CI | `terraform/` + GitHub Actions | Implementado |
| Redshift, RDS, DynamoDB, Step Functions, streaming | comparações abaixo | Somente conceito |

## RAW, CURATED e Medallion

**O que é:** RAW preserva o dado recebido, com mínima interferência. CURATED aplica contrato, tipos, deduplicação e organização para consumo. Medallion é um padrão comum de refinamento progressivo, frequentemente chamado bronze/silver/gold, não uma obrigação de nomes ou de três camadas.

**Onde apareceu no projeto:** a resposta JSON da API vai para `raw/open_meteo`; a fato e a dimensão tipadas vão para `curated` em Parquet.

**O que observei:** é possível reprocessar a CURATED sem chamar novamente a fonte. Duas camadas cobrem a necessidade pedagógica.

**Por que escolhemos isso:** separar preservação de consumo mostra rastreabilidade sem criar uma terceira camada vazia.

**Alternativa:** bronze/silver/gold ou uma única camada.

**Quando eu escolheria outra coisa:** adicionaria gold para métricas de negócio reutilizadas por vários consumidores; usaria uma camada só em um script descartável sem necessidade de reprocessamento.

## Python puro vs PySpark e processamento distribuído

**O que é:** Python comum executa o processo em uma máquina. Spark divide dados em partitions; o driver cria o plano e coordena tasks, e executors processam partitions. Escalonamento horizontal adiciona máquinas/workers, não apenas CPU/RAM a uma máquina.

**Onde apareceu no projeto:** Python faz chamadas HTTP e grava os poucos objetos RAW; Glue/PySpark explode arrays, tipa, valida, deduplica e escreve Parquet.

**O que observei:** neste dataset minúsculo, Spark tem overhead de inicialização maior do que a transformação. Ele não foi escolhido por ser “mais rápido”.

**Por que escolhemos isso:** Python é suficiente para I/O simples e controle explícito. Spark foi incluído para praticar um modelo que escala quando os dados deixam de caber/processar confortavelmente em uma máquina ou quando o ecossistema Glue já é o padrão do time.

**Alternativa:** Python + Pandas/Polars, SQL em Athena ou DuckDB.

**Quando eu escolheria outra coisa:** Python/Polars para alguns GB em uma máquina adequada e fluxo simples; Spark para grande volume, transformações distribuídas, muitos arquivos e integração já operada em cluster/serverless Spark.

## Lazy evaluation

**O que é:** transformations como `filter`, `select` e `join` montam um plano; actions como `count`, `collect` e escrita disparam a execução. O logical plan descreve o resultado; o optimizer o reescreve; o physical plan escolhe operadores concretos.

**Onde apareceu no projeto:** `glue/spark_experiments.py` cria `lazy`, chama `explain("formatted")` e só depois executa `count()`.

**O que observei:** não há materialização na definição de `lazy`; o plano aparece antes da action.

**Por que escolhemos isso:** um exemplo curto torna clara a separação entre declarar e executar.

**Alternativa:** execução eager de listas/Pandas.

**Quando eu escolheria outra coisa:** eager é mais intuitivo para transformações locais pequenas; lazy permite otimização global e evita trabalho que não será consumido.

## Partitions, tasks e paralelismo

**O que é:** em regra, uma task processa uma partition por estágio. `repartition` redistribui dados e pode aumentar/reduzir partitions com shuffle; `coalesce` reduz partitions normalmente com menos movimentação.

**Onde apareceu no projeto:** o experimento imprime 4 partitions iniciais, 8 após `repartition` e 2 após `coalesce`.

**O que observei:** partitions demais geram agendamento e arquivos pequenos; de menos deixam workers ociosos e criam tasks grandes.

**Por que escolhemos isso:** números contrastantes ensinam o equilíbrio sem sugerir um número mágico.

**Alternativa:** deixar Spark decidir com configurações adaptativas.

**Quando eu escolheria outra coisa:** ajustaria após observar tamanho das partitions, tempo de tasks, skew e capacidade real do cluster.

## Shuffle e redução antecipada

**O que é:** shuffle redistribui registros entre executors para reunir chaves iguais. Usa rede e pode usar memória/disco, criando serialização, transferência e espera.

**Onde apareceu no projeto:** `groupBy`, `repartition` e join; no `explain`, procure `Exchange`.

**O que observei:** uma agregação por chave cria uma fronteira de troca de dados.

**Por que escolhemos isso:** `filter` cedo e `select` só das colunas necessárias reduzem os bytes que atravessam essa fronteira.

**Alternativa:** agregações locais/parciais, particionamento compatível ou broadcast para lookup pequeno.

**Quando eu escolheria outra coisa:** shuffle é inevitável para várias operações globais; o objetivo é reduzir dados e evitar shuffles redundantes, não eliminar todos.

## Broadcast join

**O que é:** copia uma relação pequena para cada executor e mantém a tabela grande onde está, evitando redistribuí-la por chave.

**Onde apareceu no projeto:** `base.join(F.broadcast(lookup), "group_id")`; procure `BroadcastExchange` e `BroadcastHashJoin`.

**O que observei:** o plano contrasta o join comum com o hint explícito. O otimizador pode decidir broadcast sozinho conforme estatísticas e limiares.

**Por que escolhemos isso:** `dim_location` representa naturalmente um lookup pequeno.

**Alternativa:** sort-merge/shuffle hash join.

**Quando eu escolheria outra coisa:** não faria broadcast se a dimensão não coubesse com segurança na memória de cada executor ou mudasse de tamanho de modo imprevisível.

## Data skew

**O que é:** distribuição desigual de registros por chave. Uma task “quente” demora enquanto as demais terminam.

**Onde apareceu no projeto:** o experimento sintético envia 90% das linhas para `skew_key = 'hot'`.

**O que observei:** ter várias partitions não garante paralelismo útil quando uma chave concentra os dados.

**Por que escolhemos isso:** o fenômeno não aparece claramente no pequeno dataset real, então a simulação é honesta e controlada.

**Alternativa:** salting, pré-agregação, broadcast, tratamento separado da chave quente ou Adaptive Query Execution.

**Quando eu escolheria outra coisa:** a estratégia depende da operação, do tamanho e de o skew ser estável ou episódico.

## Small files

**O que é:** muitos objetos pequenos aumentam listagem, abertura, metadados e agendamento por pouco trabalho útil.

**Onde apareceu no projeto:** o mesmo conjunto é escrito em 24 arquivos e em 2 arquivos; tempos de escrita/leitura são impressos.

**O que observei:** mesmo com poucos dados, há mais arquivos/tasks. No S3, Spark e Athena, esse overhead cresce com a quantidade de objetos.

**Por que escolhemos isso:** dezenas de arquivos já mostram o efeito sem fabricar milhares.

**Alternativa:** compactação periódica, `coalesce`, `repartition` consciente ou formatos de tabela com manutenção.

**Quando eu escolheria outra coisa:** não compactaria agressivamente se isso produzisse arquivos gigantes, pouco paralelismo ou reescrita cara.

## Parquet, column pruning e predicate pushdown

**O que é:** Parquet é colunar, armazena schema e suporta compressão. O leitor pode ler só as colunas pedidas (column pruning) e pular grupos incompatíveis com filtros por estatísticas (predicate pushdown).

**Onde apareceu no projeto:** a CURATED é escrita em Parquet; a RAW continua JSON.

**O que observei:** comparar `du -sh data/raw data/curated` mostra tamanho físico, mas neste volume o overhead de arquivos pode distorcer a relação.

**Por que escolhemos isso:** é adequado a análises que leem subconjuntos de colunas e reduz scan no Athena.

**Alternativa:** JSON/CSV para intercâmbio e depuração; Avro para registros/eventos; Iceberg quando transações e evolução de tabela justificarem.

**Quando eu escolheria outra coisa:** manteria JSON na borda/raw e evitaria Parquet para mensagens individuais minúsculas.

## Particionamento no S3 e partition pruning

**O que é:** pastas `year/month/day` viram partitions de catálogo. Um filtro explícito nessas colunas permite ignorar diretórios inteiros.

**Onde apareceu no projeto:** `fact_weather_hourly/year=.../month=.../day=...`; as duas primeiras queries de `sql/02_learning_queries.sql` comparam scan total e de um dia.

**O que observei:** ler todo janeiro processou 13.426 bytes; filtrar `year=2025/month=1/day=15` processou 457 bytes. A redução observada foi de 96,6%. O uso de `avg(temperature_c)` foi intencional: `count(*)` isolado havia sido respondido por metadados com 0 bytes e não demonstrava leitura dos Parquets.

**Por que escolhemos isso:** tempo é filtro natural e tem cardinalidade administrável neste caso.

**Alternativa:** particionar por mês ou usar partition projection.

**Quando eu escolheria outra coisa:** evitaria `location_id` de cardinalidade muito alta ou timestamp por hora, que criariam diretórios/arquivos demais.

## Glue, Data Catalog e Athena

**O que é:** S3 guarda bytes. Glue executa a transformação Spark. O Data Catalog guarda database, table, schema e partitions. Athena usa esses metadados para interpretar arquivos e executar SQL serverless. Crawler infere metadados; DDL explícito declara o contrato.

**Onde apareceu no projeto:** Terraform cria database e job; `sql/01_catalog.sql` cria duas tables e descobre partitions com `MSCK REPAIR TABLE`.

**O que observei:** o Glue terminou com `SUCCEEDED`; o catálogo expôs a fato com 2.976 linhas e a dimensão; `MSCK REPAIR TABLE` registrou as partitions. O arquivo não se torna “tabela” por estar no S3: o catálogo adiciona a interpretação.

**Por que escolhemos isso:** DDL explícito é barato, visível e evita crawler para duas tabelas conhecidas.

**Alternativa:** crawler para fontes numerosas/mutáveis ou IaC completo das tables.

**Quando eu escolheria outra coisa:** usaria crawler para descoberta inicial; manteria schema explícito quando contrato e revisão importam.

## Consulta ad hoc e Athena vs Redshift

**O que é:** ad hoc é uma consulta criada para uma pergunta específica naquele momento, não um workload fixo e previsível. Athena consulta arquivos diretamente e cobra principalmente por scan. Redshift é um warehouse para serving estruturado, recorrência, concorrência e desempenho mais previsível.

**Onde apareceu no projeto:** SQL explora contagem, duplicidade, agregação e joins no lake.

**O que observei:** Parquet, seleção de colunas e partition pruning conectam desempenho a custo.

**Por que escolhemos isso:** Athena não exige cluster e combina com uso esporádico.

**Alternativa:** Redshift, BigQuery/Snowflake ou um banco local analítico.

**Quando eu escolheria outra coisa:** Redshift ganha força com dashboards concorrentes, SLAs e workload repetitivo; não foi provisionado para uma comparação que pode ser aprendida conceitualmente.

## Modelagem dimensional e OLAP

**O que é:** fato representa eventos/medidas em uma granularidade declarada; dimensão traz contexto descritivo. OLAP favorece leitura analítica, agregação e histórico. Sistemas operacionais favorecem transações e estado atual.

**Onde apareceu no projeto:** fato por localização-hora e dimensão por localização.

**O que observei:** separar cidade/estado evita repetir atributos descritivos em toda análise e oferece um join real.

**Por que escolhemos isso:** uma fato + uma dimensão é suficiente para revisar granularidade e chave.

**Alternativa:** tabela larga única, modelo normalizado ou Data Vault.

**Quando eu escolheria outra coisa:** uma tabela larga pode ser melhor para consumo simples; dimensional cresce em valor com métricas e dimensões compartilhadas.

## RDS vs DynamoDB vs analytics

**O que é:** RDS atende workload relacional/transacional com SQL e integridade; DynamoDB é NoSQL orientado a padrões de acesso e baixa latência; S3/Athena/Redshift atendem analytics em diferentes formatos de custo e serving.

**Onde apareceu no projeto:** somente S3/Athena são necessários.

**O que observei:** tecnologia deve seguir workload; adicionar banco operacional não melhora um pipeline analítico de arquivos.

**Por que escolhemos isso:** RDS e DynamoDB seriam custos e conceitos desconectados.

**Alternativa:** usá-los como fonte operacional em um projeto futuro.

**Quando eu escolheria outra coisa:** RDS para transações relacionais; DynamoDB quando as consultas principais estão conhecidas e escala/latência justificam NoSQL.

## Lambda vs Glue

**O que é:** Lambda executa código curto e orientado a evento; Glue fornece runtime e recursos de processamento de dados, inclusive Spark distribuído. A cota atual de duração de uma invocação Lambda é 900 segundos; confirme sempre na [documentação oficial](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html).

**Onde apareceu no projeto:** a Lambda opcional valida um evento de objeto RAW; o Glue transforma todo o conjunto.

**O que observei:** uma reação pequena cabe na Lambda, mas isso não a transforma em substituta de um job Spark.

**Por que escolhemos isso:** a fronteira é mais didática do que executar duas transformações iguais em serviços diferentes.

**Alternativa:** EventBridge/Step Functions para disparo, ou Python sem Spark no Glue.

**Quando eu escolheria outra coisa:** Lambda para eventos curtos e limitados; Glue para leitura/escrita de datasets, integração de catálogo e processamento mais longo/distribuído.

## Airflow vs Step Functions

**O que é:** Airflow orquestra DAGs com tasks, dependências, retry, schedule e backfill; não deveria carregar o processamento pesado dentro do scheduler. Step Functions orquestra serviços AWS como máquina de estados gerenciada.

**Onde apareceu no projeto:** `airflow/weather_pipeline_dag.py` encadeia ingest → valida RAW → Glue → valida CURATED → Athena.

**O que observei:** o DAG descreve ordem e recuperação; Glue continua sendo o motor da transformação.

**Por que escolhemos isso:** provisionar MWAA/Airflow completo seria desproporcional. Código + validação ensinam a fronteira.

**Alternativa:** Step Functions para integração AWS nativa e pouco código operacional.

**Quando eu escolheria outra coisa:** Airflow para ecossistema de dados, backfills e DAGs complexos; Step Functions para workflows serverless/event-driven fortemente AWS.

## Batch vs streaming e os 4 Vs

**O que é:** batch processa conjuntos em intervalos; streaming processa eventos contínuos com baixa latência. Os 4 Vs são Volume, Velocity, Variety e Veracity.

**Onde apareceu no projeto:** o pipeline é batch de dados horários. Volume e velocity são baixos; variety é baixa (uma API); veracity importa porque dados meteorológicos de reanálise são estimativas e precisam de contrato/qualidade.

**O que observei:** nenhum V transforma este lab em big data.

**Por que escolhemos isso:** não há requisito de segundos nem eventos contínuos.

**Alternativa:** Kinesis/Kafka e processamento streaming.

**Quando eu escolheria outra coisa:** quando atraso de lote viola o negócio e eventos chegam continuamente em escala/velocidade que exigem processamento incremental.

## Data Quality

**O que é:** regras vêm antes da ferramenta. O job verifica schema, nulos críticos, chave duplicada, umidade 0–100, precipitação/vento não negativos e weather code em domínio.

**Onde apareceu no projeto:** `require_raw_schema`, `quality_metrics` e testes do job.

**O que observei:** duplicatas são medidas e deduplicadas; valor inválido faz o pipeline falhar de forma controlada antes da CURATED.

**Por que escolhemos isso:** falhar é mais claro que publicar dado inválido; quarentena seria útil se o negócio aceitasse dados parciais.

**Alternativa:** Glue Data Quality, Great Expectations, Soda ou Deequ.

**Quando eu escolheria outra coisa:** adotaria ferramenta quando regras, datasets, relatórios e owners crescerem; freshness passaria a ser crítica em ingestão recorrente.

## Idempotência e falhas

**O que é:** a mesma execução lógica não deve criar resultado diferente ou duplicidade indevida. Retry só faz sentido para falha transitória; schema inválido deve falhar até ser corrigido.

**Onde apareceu no projeto:** a chave RAW é determinística por fonte/período/local; a reexecução substitui o objeto. A fato deduplica por `location_id + observed_at`, mantendo a ingestão mais recente. A chamada HTTP usa três tentativas com backoff.

**O que observei:** testes comprovam uma única chave após rerun, deduplicação e falha com domínio inválido.

**Por que escolhemos isso:** é uma estratégia pequena, visível e suficiente para o lab.

**Alternativa:** manifestos de execução, checkpoint, merge em Iceberg/Hudi/Delta ou chave idempotente em banco.

**Quando eu escolheria outra coisa:** cargas incrementais concorrentes e updates históricos pediriam uma tabela transacional/controle mais forte.

## Terraform / IaC e infraestrutura efêmera

**O que é:** IaC torna infraestrutura versionável, revisável e repetível. O ciclo é `init → plan → apply → destroy`.

**Onde apareceu no projeto:** S3, database do catálogo, workgroup Athena com limite de scan, IAM e Glue Job.

**O que observei:** Glue abstrai servidores e cluster. Isso não significa ausência de compute/custo. EMR exporia mais decisões de cluster, mas não é necessário aqui.

**Por que escolhemos isso:** a fatia é pequena e real, suficiente para aprender dependências e estado.

**Alternativa:** CloudFormation/CDK ou criação manual.

**Quando eu escolheria outra coisa:** CDK quando o time prefere linguagens; CloudFormation em ambientes padronizados AWS. Infra efêmera funciona quando estado/dados importantes estão protegidos fora do recurso destruído.

## CI

**O que é:** automação em cada push/PR para detectar regressões.

**Onde apareceu no projeto:** `.github/workflows/tests.yml` instala dependências e roda `pytest -q`.

**O que observei:** CI testa lógica, não implanta recursos nem usa credenciais AWS.

**Por que escolhemos isso:** `push → pytest` ensina o propósito sem criar CD complexo.

**Alternativa:** adicionar lint, análise estática e `terraform validate`.

**Quando eu escolheria outra coisa:** aumentaria gates conforme risco, equipe e frequência de mudança.

## FinOps e custo

**O que é:** custo é consequência arquitetural observável. Parquet/column pruning e partition pruning reduzem scan; menos shuffle reduz compute; serverless evita recurso permanentemente ocioso.

**Onde apareceu no projeto:** job curto com dois workers, bucket pequeno, lifecycle de resultados Athena e limite de 100 MiB por query.

**O que observei:** preços oficiais consultados em 9 de setembro de 2026: Athena a US$5/TB escaneado e Glue ETL a US$0,44/DPU-hora em `us-east-1`; preços variam por região. O job usa 2 workers G.1X. Uma execução de 1 minuto custa aproximadamente US$0,015; 5 minutos, US$0,073, além de valores residuais de S3/Athena. Consulte [Athena Pricing](https://aws.amazon.com/athena/pricing/) e [Glue Pricing](https://aws.amazon.com/glue/pricing/) antes de executar novamente.

**Por que escolhemos isso:** uma única execução real do Glue ensina mais do que sessões interativas deixadas abertas. Data Catalog fica muito abaixo da faixa gratuita de um milhão de objetos/acessos.

**Alternativa:** executar todas as experiências Spark localmente e usar AWS só para o pipeline final.

**Quando eu escolheria outra coisa:** workloads contínuos podem justificar capacidade comprometida; este laboratório não justifica.

## Ordem recomendada de exploração

1. Leia `src/ingest.py` e execute duas vezes; confirme que existem apenas quatro JSONs para o mesmo período.
2. Leia `glue/weather_transform.py`, rode os testes e force a umidade 150 no teste de falha.
3. Compare RAW JSON com CURATED Parquet e inspecione a granularidade fato/dimensão.
4. Rode `spark_experiments.py` procurando `Exchange`, `BroadcastHashJoin` e os números de partitions.
5. Na AWS, observe o Glue Job Run, seus logs e o relatório de qualidade.
6. Crie as tabelas no Athena e compare bytes escaneados com/sem filtro de partition.
7. Leia o DAG Airflow e explique por que ele orquestra, mas não transforma os dados.
8. Execute `terraform plan`; só depois `apply`. Ao final, revise e execute `destroy`.

Em entrevista, não diga “Spark porque é mais rápido”. Diga qual era o volume, reconheça que Python bastava aqui e use os planos/experimentos para explicar quando distribuição, shuffle, skew e layout de arquivos passam a importar.
