# AWS Build Log

Execução inicial: 9 de setembro de 2026, região `us-east-1`.

## Resultado confirmado

- Terraform: 9 recursos planejados e criados.
- Glue Job: `SUCCEEDED`.
- Período: 1–31 de janeiro de 2025, quatro cidades.
- Fato no Athena: 2.976 observações (`31 dias × 24 horas × 4 cidades`).
- Partition de 15 de janeiro: 96 observações.
- Join fato + dimensão: retornou Belo Horizonte, Blumenau, Rio de Janeiro e São Paulo.
- Validação ad hoc de chave `location_id + observed_at`: nenhuma duplicidade encontrada.
- Scan observado no join analítico: 25.150 bytes.
- Scan observado na validação de duplicidade: 12.311 bytes.
- Scan de todas as partitions com `avg(temperature_c)`: 13.426 bytes.
- Scan de uma partition diária com `avg(temperature_c)`: 457 bytes.
- Redução observada com partition pruning: 96,6%.

## Aprendizado surgido durante a execução

A comparação inicial de partition pruning usava apenas `count(*)`. O Athena respondeu às duas consultas com 0 bytes escaneados, aproveitando metadados, então esse resultado não comprovava pruning dos dados Parquet.

As consultas foram corrigidas para calcular também `avg(temperature_c)`. Isso força leitura de uma coluna de dados e permite comparar honestamente todas as partitions com uma única partition diária. Na segunda execução, o scan caiu de 13.426 para 457 bytes.

Os resultados retornados foram coerentes com a cardinalidade esperada: 2.976 linhas no mês e 96 no dia filtrado. A consulta de duplicidade retornou somente o cabeçalho, confirmando zero chaves repetidas.

## Custo

Não foi provisionado compute permanente. O custo exato deve ser confirmado no Billing, mas, pela duração observada e pela configuração de dois workers G.1X, a execução do Glue deve consumir apenas alguns centavos de dólar; S3 e Athena são residuais neste volume.

## Privacidade da evidência

O repositório guarda apenas o resumo técnico necessário para comprovar o laboratório. IDs de consultas, nome físico do bucket e identificadores da conta foram omitidos por não acrescentarem valor didático.
