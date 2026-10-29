# Projeto 1 — Pipeline de qualidade de dados

Pipeline batch reproduzível para analisar a relação entre prazo de entrega e avaliação de clientes no Brazilian E-Commerce Public Dataset by Olist.

## Sobre este repositório

Este README é a documentação pública de instalação, arquitetura e execução. O repositório contém o pipeline local e os contratos das fontes; dados brutos, saídas de execução, instruções internas para agentes e notas pessoais ficam fora do versionamento.

## Pergunta analítica

Como o desempenho da entrega — comparando datas estimadas e reais — se relaciona às notas de avaliação? A análise deve preservar os casos sem entrega ou sem avaliação e controlar a granularidade para não duplicar métricas.

## Arquitetura prevista

```text
CSV original → Bronze (Parquet) → Silver (dbt + DuckDB) → Gold → publicação aprovada
```

O MVP é local e batch sob demanda, orquestrado pelo Airflow. A DAG local está implementada; publicação remota continua fora até a escolha de destino.

## Dados

Os CSVs da Olist devem estar na pasta local `dataset-ecommerce-brasil/`. O `.gitignore` exclui essa pasta e dados derivados para evitar versionar datasets e artefatos. O índice e os contratos em `contracts/` declaram as fontes incluídas e o grão de cada uma.

IDs devem ser lidos como texto. Pagamentos e avaliações têm cardinalidade que precisa ser medida e tratada antes de joins. Não inclua geolocalização ou marketing no MVP sem revisar o escopo.

## Ambiente e execução Bronze

Use Python 3.11 ou superior e [uv](https://docs.astral.sh/uv/). O `pyproject.toml` declara as dependências diretas e o `uv.lock` fixa a resolução reproduzível:

```bash
uv sync
uv run olist-bronze --batch-id olist-mvp-2026-10-25
```

O comando valida os oito CSVs definidos em `contracts/olist_mvp.yaml` e escreve um Parquet por fonte em `data/bronze/<batch_id>/`, junto com `ingestion_report.json`. Use o mesmo `batch_id` para substituir o diretório do lote anterior depois de montar a nova versão em uma pasta temporária; outro identificador cria outro lote. Assim não se acumulam cópias do mesmo lote, mas `_ingested_at` muda a cada execução. A rotina tenta restaurar o lote anterior se a promoção falhar, mas a troca envolve mais de uma renomeação e não promete recuperação automática após queda abrupta do processo. CSVs ausentes, colunas obrigatórias ausentes, nulos em campos não anuláveis, tipos lógicos inválidos, chaves repetidas ou linhas malformadas bloqueiam a gravação. Colunas extras são mantidas. Valores de origem são gravados como texto; datas permanecem sem conversão/fuso. Geolocalização não é processada.

Silver, Gold e DAG local do Airflow estão implementadas; a publicação remota continua pendente de decisão.

## Fora do MVP

Spark, Kafka, Iceberg, Delta Lake, object storage, ingestão contínua, GitHub Actions e Projeto 2 ficam fora deste MVP. Airflow local foi escolhido por Henrique; Neon continua fora até a publicação ser confirmada.


## Implementação atual: contratos e Bronze

Esta seção descreve somente o que existe no código-fonte. Silver, Gold e Airflow local estão implementados. A publicação remota no Neon continua pendente de decisão.

### Fluxo executável

```text
contracts/olist_mvp.yaml + contratos individuais
        ↓ load_contracts
CSV local ──→ validate_sources ── falha? relatório de bloqueio e encerramento
        ↓ aprovado
pandas.read_csv (texto) → metadados → Parquet via PyArrow
        ↓
relatório JSON + substituição do diretório do batch_id
```

O comando parte da raiz do repositório:

```bash
uv sync
uv run olist-bronze --batch-id olist-mvp-2026-10-25
```

`--batch-id` identifica o lote lógico. Repetir o mesmo identificador substitui sua pasta; usar outro cria outra pasta. Os padrões são `contracts/olist_mvp.yaml` e `data/bronze`, configuráveis por `--contracts` e `--output-dir`. A validação percorre os CSVs e a ingestão os lê novamente; por isso a premissa operacional é que os arquivos de origem não mudem durante a execução.

### Código-fonte e responsabilidades

- `contracts.py`: `ContractDefinitionError` representa erro na definição YAML; `_matches_logical_type` verifica se cada valor atende ao tipo lógico sem transformá-lo; `load_contracts` lê e valida o índice e os oito contratos; `validate_sources` verifica a estrutura e o conteúdo dos CSVs e devolve um relatório sem incluir valores das linhas. A validação registra números de linha de amostra, não os valores pessoais nelas.
- `bronze.py`: `IngestionBlocked` sinaliza dados reprovados pelos contratos; `_write_json` grava relatórios por arquivo temporário e substituição; `ingest_batch` valida, monta Parquets em staging, grava relatório e promove a pasta do lote; `main` interpreta argumentos CLI, mostra resumo e retorna código de saída 0 ou 1.
- `tests/test_contracts_and_bronze.py`: fixtures pequenas exercitam aceitação de colunas extras, bloqueio de coluna ausente/nulo proibido, substituição do mesmo lote sem acumular arquivos e bloqueio da gravação com relatório quando a entrada é inválida.

### Funções e decisões importantes

`_matches_logical_type(value, logical_type)` aceita texto sem conversão; tenta `int` para inteiro; usa `Decimal` e exige valor finito para decimal; e para timestamp exige formato de data/hora ISO reconhecido, sem fuso. A checagem de timestamp combina expressão regular e `datetime.fromisoformat`: a expressão limita o formato esperado e o parser rejeita datas impossíveis. A função retorna falso para tipos não suportados ou valores inválidos.

`load_contracts(index_path)` resolve o caminho do índice, carrega YAML com `yaml.safe_load`, confirma exatamente oito arquivos/contratos, impede caminhos nos nomes, duplicações, inconsistência entre arquivos incluídos e contratos, inclusão de geolocalização e chaves vazias, repetidas, não declaradas ou anuláveis. Também confirma os tipos lógicos permitidos e retorna a pasta de origem junto da lista dos contratos. Erros de leitura ou YAML são convertidos em `ContractDefinitionError`, preservando a causa original.

`validate_sources(raw_dir, contracts)` processa cada fonte. Verifica existência e cabeçalho, cabeçalhos duplicados, colunas obrigatórias ausentes, colunas extras (aceitas e reportadas), colisão com nomes reservados de metadados, largura de cada linha, campos nulos proibidos, tipo lógico e unicidade da chave. Não para no primeiro problema: acumula regras e amostras de números de linha para diagnóstico. O retorno contém `passed`, detalhes por arquivo, `issue_count` (regras/problemas registrados) e `error_count` (ocorrências, com valor padrão 1 para regras sem contagem explícita). A escala CSV conta registros lógicos, não necessariamente linhas físicas quando há campos multilinha entre aspas.

`ingest_batch(...)` primeiro restringe `batch_id` a 1–100 caracteres seguros para nomes de pasta. Carrega contratos e valida todas as fontes antes de escrever Parquet. Define horário UTC, cria relatório com contagens e caminhos; se reprovado, salva em `data/bronze/_reports/<batch_id>.json` e lança `IngestionBlocked`. Se aprovado, lê cada CSV com pandas como string, desativa inferência de valores ausentes, verifica se a contagem ainda bate com a validação, acrescenta `_batch_id`, `_ingested_at` e `_source_file`, e grava Parquet usando engine PyArrow. A pasta temporária é promovida ao destino; se já existe uma saída do mesmo lote, ela é movida para backup e restaurada se a promoção falhar. O backup é removido após sucesso. Erros durante a execução limpam staging quando possível, registram falha e relançam a exceção.

A reexecução é idempotente no sentido operacional de não anexar uma cópia adicional do lote: a pasta anterior é substituída. Não é uma transação de filesystem com garantia contra queda de energia entre renomeações, nem produz arquivos idênticos byte a byte: o timestamp de ingestão muda. Não há coordenação para duas execuções concorrentes com o mesmo `batch_id`. Se a validação falhar numa tentativa posterior, a saída anteriormente bem-sucedida permanece; consumidores devem conferir o relatório da tentativa, para não confundir uma saída antiga com o resultado atual.

`main()` usa `argparse` para os argumentos, captura erros esperados de contrato/ingestão/arquivo/valor, escreve a mensagem em stderr e retorna 1. No sucesso imprime JSON resumido e retorna 0. `if __name__ == "__main__"` permite execução como módulo/script; o entry point `olist-bronze` do `pyproject.toml` chama a mesma função.

### Bibliotecas e ferramentas

| Dependência | Uso efetivo |
|---|---|
| Python padrão: `csv` | Percorre CSVs e detecta estrutura antes da ingestão. `newline=""` deixa o módulo tratar corretamente as convenções de newline; `utf-8-sig` aceita UTF-8 com BOM. |
| `pathlib.Path` | Manipula caminhos e operações de arquivo de forma independente de concatenação manual de strings. |
| `re` | Valida o formato do timestamp e o identificador seguro do lote. |
| `datetime` / `UTC` | Valida timestamps sem fuso na origem e registra o momento da ingestão em UTC. |
| `decimal.Decimal` | Valida números decimais sem a aproximação binária típica de `float`. |
| `typing.Any` | Tipagem auxiliar para estruturas carregadas de YAML/JSON, cujo formato é dinâmico. |
| `json` | Serializa relatório legível por máquina. |
| `uuid` | Gera nomes temporários únicos para gravação, staging e backup. |
| `shutil` | Remove staging/backup de diretórios após falhas ou promoção concluída. |
| `argparse` / `sys` | Interface de linha de comando e mensagens/códigos de erro. |
| `PyYAML` (`yaml.safe_load`) | Lê contratos YAML sem construir objetos arbitrários do Python. YAML é configuração declarativa; a lógica executada continua no Python. |
| `pandas` | Lê cada arquivo para um DataFrame e preserva campos como strings antes da gravação. Não faz análise de entrega nesta etapa. |
| `PyArrow` | Engine que `DataFrame.to_parquet` usa para serializar os dados no formato Parquet. |
| `uv` | Resolve/instala dependências declaradas; `uv.lock` fixa a resolução para instalação reproduzível. Não executa a lógica de qualidade. |
| `pytest` | Executa os testes automatizados. |
| `Ruff` | Formata e analisa problemas comuns de Python. |
| `ty` | Verifica estaticamente compatibilidade de tipos. |

### Resultado documentado e limites

Uma execução registrada do lote `olist-mvp-2026-10-25` processou oito fontes e 550.759 linhas, sem achados ou erros no relatório. Comparações CSV/Parquet não encontraram divergências, e a reexecução do lote foi verificada.

A Bronze preserva os campos como texto. Ela não converte datas ou valores monetários para tipos analíticos, não calcula métricas, não agrega pagamentos/avaliações e não cria tabelas relacionais. Isso pertence às etapas futuras. O carregamento atual usa pandas para ler cada CSV integralmente em memória; para arquivos muito maiores, leitura em chunks ou outro desenho de memória precisaria ser avaliado.


### Silver e Gold implementadas

A transformação usa dbt com DuckDB local. O `dbt-duckdb` lê os Parquets diretamente, sem copiá-los para tabelas de entrada. O comando `make transform` passa ao dbt o diretório do lote `BRONZE_DIR`.

- **Silver (views):** `stg_orders` converte os timestamps necessários e mantém uma linha por pedido; `stg_order_reviews` converte a nota e preserva cada avaliação no grão de origem. Comentários livres e campos fora da pergunta analítica não seguem para esses modelos.
- **Gold (tabelas):** `fct_delivery_reviews` agrega as avaliações por pedido antes do `LEFT JOIN`, preservando pedidos sem avaliação e garantindo uma linha por pedido. `mart_delivery_review_summary` resume por faixa de entrega.
- **Faixas:** `early` significa entrega antes da data estimada; `on_time`, no mesmo dia; `late`, depois da data estimada; `not_delivered`, sem data real de entrega; `delivered_missing_timestamp`, status `delivered` sem data real. `delivery_delay_days` é negativo para antecipação, zero no dia e positivo para atraso. A comparação usa datas de calendário.
- **Nota:** várias avaliações do mesmo pedido são primeiro resumidas pela média. A Gold calcula a média dessas médias, dando o mesmo peso a cada pedido avaliado, e apresenta separadamente a quantidade de pedidos avaliados e de registros de avaliação.
- **Qualidade:** `dbt build` executa modelos e testes de nulos, unicidade, chave composta de avaliação, domínio da nota, relacionamento avaliação→pedido, preservação de contagem de pedidos e reconciliação da Gold. Falha retorna código não-zero. Não há etapa de publicação remota, então os resultados ficam no banco local.

Para executar tudo em sequência, com falha da Bronze impedindo a transformação:

```bash
make pipeline BATCH_ID=olist-mvp-2026-10-25
```

O banco DuckDB fica em `data/warehouse.duckdb` (ignorado pelo Git). `make transform` pode ser usado para reconstruir as camadas sobre uma Bronze já existente.


## Orquestração local com Airflow

Henrique escolheu Airflow como orquestrador. A DAG `olist_delivery_review_pipeline` tem duas tarefas sequenciais: ingestão Bronze e `dbt build` (transformação + testes). Ambas chamam os mesmos alvos do Makefile usados na execução manual; não duplicam as regras do pipeline.

A DAG não tem calendário (`schedule=None`) nem backfill (`catchup=False`): o dataset é histórico e a execução é manual. O parâmetro `batch_id` tem validação de caracteres e tamanho; execuções simultâneas da DAG são limitadas a uma; retries automáticos ficam em zero para não repetir falhas determinísticas de contrato.

Airflow tem um ambiente `uv` separado em `airflow/`, travado com constraints oficiais para Airflow 3.3.2 e Python 3.14. Isso evita misturar as dependências de orquestração com as do pipeline analítico. O banco SQLite e logs locais ficam em `.airflow/`, ignorado pelo Git. O modo `standalone` é para desenvolvimento local, não produção; ele cria acesso administrativo local e informa as credenciais no terminal. Consulte o [Quick Start oficial do Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start.html).

Para iniciar a interface local:

```bash
make airflow
```

Para executar uma DAG de teste ponta a ponta, sem iniciar a interface:

```bash
make airflow-test
```

A DAG testada usa por padrão o lote `olist-mvp-2026-10-25`; na interface, o parâmetro `batch_id` pode selecionar outro lote presente em `data/bronze` ou reconstruir o lote usando os CSVs locais. O dataset precisa existir no caminho indicado pelos contratos.
