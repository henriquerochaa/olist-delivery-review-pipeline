# Olist Delivery Review Pipeline

Pipeline batch que roda localmente e responde a uma pergunta: como a entrega se relaciona com a nota das avaliações dos clientes? Os dados vêm do [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). O processamento usa Python/Pandas, dbt, DuckDB e Airflow, e cada etapa é protegida por contratos de dados ou por testes automatizados.

## Resultado

Nota média por pedido, segundo a entrega em relação à data estimada. A tabela cobre os 99.441 pedidos, mas a nota é calculada só sobre os 98.673 que têm avaliação:

| Entrega | Pedidos | Com avaliação | Sem avaliação | Nota média | Atraso médio (dias) |
|---|---:|---:|---:|---:|---:|
| Antes do prazo (`early`) | 88.649 | 88.168 | 481 | 4,29 | −13,7 |
| No dia (`on_time`) | 1.292 | 1.280 | 12 | 4,04 | 0 |
| Atrasada (`late`) | 6.535 | 6.382 | 153 | 2,27 | +10,6 |
| Não entregue (`not_delivered`) | 2.957 | 2.835 | 122 | 1,75 | n/d |
| `delivered` sem data real (`delivered_missing_timestamp`) | 8 | 8 | 0 | 4,50 | n/d |
| **Total** | **99.441** | **98.673** | **768** | | |

Os pedidos atrasados têm nota média de 2,27, contra 4,29 nos entregues antes do prazo. Esses números descrevem um snapshot histórico e não provam causalidade: pedidos atrasados podem diferir dos demais em outros aspectos. Cada pedido pesa o mesmo na média, e quando há várias avaliações do mesmo pedido entra a média delas. Os 768 pedidos sem avaliação (coluna "Sem avaliação") e os 2.965 sem data real de entrega (as duas últimas linhas) foram mantidos, sem descarte; os sem avaliação só ficam de fora do cálculo da nota média.

## Arquitetura

```text
CSV original → Bronze (Parquet) → Silver (dbt + DuckDB) → Gold → Postgres (Neon)
        └─────────── orquestrado por Airflow (DAG local, manual) ───────────┘
```

| Camada | O que faz |
|---|---|
| Contratos (`contracts/`) | Declaram colunas, tipos lógicos, nulabilidade, chaves e grão de cada uma das 8 fontes. |
| Bronze | Valida os CSVs contra os contratos e grava um Parquet por fonte, com os campos de origem como texto e os metadados `_batch_id`, `_ingested_at` e `_source_file`. Falha de contrato bloqueia a gravação e as etapas seguintes. |
| Silver (views dbt) | `stg_orders` e `stg_order_reviews`: tipos convertidos, só as colunas necessárias. |
| Gold (tabelas dbt) | `fct_delivery_reviews` (uma linha por pedido, avaliações agregadas antes do join) e `mart_delivery_review_summary` (resumo por faixa de entrega). |
| Publicação | `publish_gold` copia as duas tabelas Gold para o schema `gold` de um Postgres no Neon, numa única transação (full refresh) com contagens reconciliadas. Só roda depois dos testes dbt, recusa um `dbt build` com item reprovado e recusa uma Gold mais antiga que a última publicada. |
| Orquestração | DAG do Airflow com `ingest_bronze` → `build_and_test_models` → `publish_gold`, sem agendamento (o dataset é histórico). |

### Decisões de projeto

- O grão de cada tabela é explícito: pedidos por `order_id`, itens por (`order_id`, `order_item_id`). Alguns pedidos têm mais de uma avaliação, então elas são agregadas antes do join com o pedido, para não duplicar métricas.
- IDs e CEPs são lidos como texto, para preservar zeros à esquerda.
- Pedidos sem entrega ou sem avaliação são mantidos e classificados. Nenhuma linha é descartada em silêncio.
- Repetir o mesmo `batch_id` substitui os artefatos do lote, sem duplicatas. O campo `_ingested_at` muda a cada execução, então os arquivos não ficam idênticos byte a byte.
- Uma falha de contrato ou de teste interrompe o fluxo.
- Geolocalização e marketing do Olist ficam fora do escopo, assim como Spark, Kafka, Iceberg, Delta Lake e object storage.

## Como executar

Requisitos: [uv](https://docs.astral.sh/uv/) e Python 3.11+ (o ambiente do Airflow, em `airflow/`, usa Python 3.14; o `uv` o resolve sozinho).

1. Baixe os CSVs do Kaggle (o repositório não os inclui; confira os termos de uso do dataset antes de redistribuir) e coloque-os em `dataset-ecommerce-brasil/`, na raiz.
2. Instale as dependências travadas e rode tudo:

```bash
uv sync
make pipeline BATCH_ID=olist-mvp-2026-10-25   # Bronze + dbt (modelos e testes)
```

Os alvos do `Makefile`:

| Comando | Função |
|---|---|
| `make bronze` | Valida os CSVs e grava a Bronze em `data/bronze/<batch_id>/`, com `ingestion_report.json`. |
| `make transform` | Roda `dbt build` sobre uma Bronze existente e grava `data/warehouse.duckdb`. |
| `make publish` | Publica a Gold do DuckDB no Postgres indicado por `NEON_DATABASE_URL`. Exige um `dbt build` anterior, sem falhas, em `dbt_pipeline_qualidade/target/run_results.json` (`--dbt-results` muda o caminho). |
| `make pipeline` | Bronze e depois dbt; a falha da primeira etapa impede a segunda. Não publica. |
| `make test` / `lint` / `format` / `typecheck` | pytest, Ruff (check e format) e ty. |
| `make airflow` | Sobe o Airflow em modo `standalone` (desenvolvimento local, não produção). |
| `make airflow-test` | Executa a DAG completa sem abrir a interface. |

Execute o dbt por `make transform` ou `make pipeline`, não por `dbt build` direto: o `Makefile` passa a variável obrigatória `bronze_dir` com o caminho do lote.

## Publicação no Postgres (Neon)

A publicação é opcional e usa uma role restrita, não a do dono do banco. A role é criada por SQL, e não pelo botão "Add role" do console, porque o console coloca a role nova no grupo `neon_superuser`, de poderes amplos; o pipeline só precisa escrever no schema `gold`. No SQL Editor do Postgres (com uma role administrativa):

```sql
CREATE SCHEMA IF NOT EXISTS gold;
CREATE ROLE pipeline_publisher WITH LOGIN PASSWORD '<senha>' NOSUPERUSER NOCREATEDB NOCREATEROLE;
GRANT USAGE, CREATE ON SCHEMA gold TO pipeline_publisher;
```

Depois exporte a connection string da role como variável de ambiente (nunca no código nem no Git) e publique:

```bash
export NEON_DATABASE_URL='postgresql://pipeline_publisher:<senha>@<host>/<banco>?sslmode=require'
make publish
```

Antes de carregar, a publicação confere o `run_results.json` do último `dbt build` e recusa Gold com item reprovado. O horário desse build fica em `gold._publish_log` (criada na primeira publicação), e uma Gold mais antiga que a última publicada é recusada; republicar a mesma é permitido. Use o host com `-pooler` (o que o console oferece por padrão); é o que foi testado, e a carga é uma única transação curta, sem estado de sessão. Use uma senha só com letras e números, ou codifique caracteres especiais na URL. Os erros do driver nunca são impressos, para não vazar credenciais. Só `order_id`, status, datas e médias de nota são publicados; não há dados de clientes.

## Testes e verificação

| Camada | Comando | O que cobre |
|---|---|---|
| Contratos e Bronze (pytest) | `make test` | Arquivo ausente, cabeçalho vazio ou duplicado, coluna obrigatória ausente, nulo proibido, tipo lógico inválido, chave simples e composta duplicada, linha malformada, `batch_id` inseguro, reexecução sem duplicar e preservação do último lote bom quando uma nova tentativa é bloqueada. |
| Publicação (pytest) | `make test` | Validação do schema, bloqueio com origem vazia ou inconsistente, variável ausente, reconciliação de contagens com rollback, bloqueio de `dbt build` reprovado ou ausente, recusa de Gold mais antiga que a última publicada e erro sem vazar a senha. Usa DuckDB temporário e um destino falso, sem Postgres real. |
| Definição da DAG (pytest) | `make test` | DAG manual, sem backfill nem retries, uma execução por vez, Bronze → transformação → publicação, falha propagada. A DAG é lida como código (AST), sem iniciar o Airflow. |
| Lógica dos modelos (dbt unit tests) | `make transform` | Faixas de entrega, atraso em dias, média de várias avaliações por pedido, pedido sem avaliação preservado e resumo com peso igual por pedido. |
| Dados reais (dbt data tests) | `make transform` | Chaves únicas, nulos, nota entre 1 e 5, avaliação→pedido, reconciliação de contagens entre Bronze, fato e resumo. |
| Ponta a ponta | `make airflow-test` | Executa a DAG completa, inclusive a publicação (exige `NEON_DATABASE_URL`). |

Última verificação registrada (02/11/2026): `pytest` com 43 testes aprovados (5 deles, do bloqueio de Gold desatualizada, também foram exercitados com uma publicação real no Neon e pela DAG); `ruff check`, `ruff format --check`, `ty check` e `uv lock --check` sem erros; Bronze com 8 fontes e 550.759 linhas; `dbt build` com `PASS=24 WARN=0 ERROR=0` (2 views, 2 tabelas, 18 testes de dados, 2 testes unitários); `make airflow-test` com as três tarefas (Bronze, dbt e publicação) em `success`, publicando 99.441 linhas na fato e 5 no resumo no Neon.

## Limitações

- Os dados são um snapshot histórico; as médias são descritivas e não permitem inferir causalidade.
- `date_diff` compara datas de calendário, não horas.
- A Bronze lê cada CSV inteiro em memória com pandas; para arquivos muito maiores seria preciso ler em blocos.
- A troca do diretório do lote usa mais de uma renomeação e não promete recuperação automática após queda abrupta do processo. Não há coordenação para duas execuções simultâneas do mesmo `batch_id`.
- Os modelos `stg_*` não têm teste unitário próprio; a regra de negócio está na Gold.
- A orquestração `standalone` com SQLite serve ao desenvolvimento local. Não há CI. A publicação é um full refresh: substitui as tabelas do schema `gold` a cada execução, sem histórico de versões. Ela também não usa área de preparo com promoção: a transação única garante tudo ou nada, mas as tabelas são substituídas no próprio schema `gold`. O tempo medido da carga (três execuções, de 5 a 9 s) é uma ordem de grandeza, sem controle de rede.

## Estrutura do repositório

```text
contracts/                 contratos YAML das 8 fontes e índice
src/projeto1_pipeline_qualidade_dados/   contratos, ingestão Bronze (olist-bronze) e publicação (olist-publish)
dbt_pipeline_qualidade/    modelos Silver/Gold, testes e testes unitários
airflow/                   DAG e ambiente uv separado do Airflow
tests/                     pytest sobre fixtures reduzidas
Makefile                   atalhos para cada etapa
```

## Licença

Código sob licença MIT (ver `LICENSE`). Os dados da Olist seguem os termos próprios do dataset no Kaggle e não são redistribuídos aqui.
