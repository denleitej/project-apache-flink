# Dados Analíticos do Flink usando Apache Flink e PyFlink

Pipeline analítica utilizando Apache Flink e PyFlink para processamento de dados públicos do PIX disponibilizados pelo Banco Central do Brasil. 

---

# Visão Geral do Projeto

Este projeto foi desenvolvido com finalidade acadêmica para demonstração prática de processamento distribuído de dados utilizando:

- Apache Flink
- PyFlink
- Python
- SQL Analytics
- Visualização de dados

A proposta consiste em construir uma pipeline local capaz de:

- ingerir datasets públicos do PIX;
- realizar transformações e agregações analíticas;
- executar processamento batch utilizando PyFlink;
- gerar análises temporais e multidimensionais;
- exportar gráficos automaticamente.

O projeto utiliza dados reais disponibilizados pelo Banco Central do Brasil através do portal oficial de Dados Abertos.

---

# Objetivos do Projeto

Este projeto demonstra:

- uso do Apache Flink em ambiente local;
- processamento analítico com PyFlink;
- integração entre Flink, Pandas e Matplotlib;
- construção de pipelines ETL simplificadas;
- análise temporal de grandes volumes financeiros;
- geração automatizada de gráficos analíticos.

---

# Dataset Utilizado

## Estatísticas do PIX — Banco Central do Brasil

Fonte oficial:

https://dadosabertos.bcb.gov.br/dataset/pix

Os dados incluem:

- volume financeiro;
- quantidade de transações;
- natureza das transações;
- regiões envolvidas;
- tipos de operação;
- estatísticas mensais.

Formato original:

- CSV compactado (.zip)

Formato processado no projeto:

- CSV consolidado

---

# Tecnologias Utilizadas

| Tecnologia | Finalidade |
|---|---|
| Apache Flink | Processamento distribuído |
| PyFlink | API Python do Flink |
| Python 3.12.10 | Linguagem principal |
| Pandas | Manipulação tabular |
| Matplotlib | Geração de gráficos |
| Java 11+ | Runtime necessário para Flink |
| venv | Ambiente virtual Python |

---

# Estrutura do Projeto

```text
flink-testing/
│
├── .venv/                         # Ambiente virtual Python
│
├── data/
│   ├── raw/                       # CSVs originais baixados
│   └── processed/
│       └── pix_consolidado.csv    # Dataset consolidado
│
├── jobs/
│   ├── pix_analysis.py            # Job batch principal
│   └── pix_stream_job.py          # Simulação de streaming
│
├── outputs/
│   └── charts/
│       ├── evolucao_pix.png
│       ├── natureza_temporal.png
│       └── regioes_pix.png
│
├── scripts/
│   ├── pix_downloader.py          # Download dos datasets
│   └── normalizar_pix.py          # Consolidação/normalização
│
├── requirements.txt
│
└── README.md

# Pré-requisitos

## Python

Versão recomendada:

Python 3.12.10
```

Verifique a versão instalada:

```bash
python --version
```

---

## Java

O Apache Flink requer Java instalado no sistema.

Versão recomendada:

```text
Java 11 ou superior
```

Verifique:

```bash
java -version
```

---

## Sistema Operacional

Compatível com:

- Windows 10/11
- Linux
- macOS

---

## Variáveis de Ambiente

É recomendado configurar:

### JAVA_HOME

Exemplo no Windows:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-17"
```

Exemplo Linux/macOS:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
```

---

# Configuração do Ambiente

## 1. Clonar o Repositório

```bash
git clone https://github.com/denleitej/project-apache-flink
```

```bash
cd project-apache-flink
```

---

## 2. Criar Ambiente Virtual

### Windows

```bash
python -m venv .venv
```

### Linux/macOS

```bash
python3 -m venv .venv
```

---

## 3. Ativar Ambiente Virtual

### Windows (PowerShell)

```powershell
.venv\Scripts\activate
```

### Linux/macOS

```bash
source .venv/bin/activate
```

---

## 4. Instalar Dependências

```bash
pip install -r requirements.txt
```

---

## Dependências Principais

```txt
apache-flink
pandas
matplotlib
requests
```

---

## 5. Verificar Instalação do PyFlink

```bash
python -c "import pyflink; print('PyFlink OK')"
```

Resultado esperado:

```text
PyFlink OK
```

---

# Fluxo Completo da Pipeline

O projeto segue o seguinte fluxo:

```text
Download dos Dados
        ↓
Normalização
        ↓
Processamento Batch com PyFlink
        ↓
Geração de Analytics
        ↓
Exportação de Gráficos
```

---

# Execução do Projeto

## Etapa 1 — Download dos Dados

Responsável por:

- baixar datasets públicos do PIX;
- salvar arquivos CSV localmente;
- organizar os dados brutos.

### Executar:

```bash
python scripts/pix_downloader.py
```

Arquivos gerados:

```text
data/raw/
```

---

## Etapa 2 — Normalização dos Dados

Responsável por:

- consolidar múltiplos CSVs;
- padronizar colunas;
- tratar inconsistências;
- gerar dataset consolidado final.

### Executar:

```bash
python scripts/normalize_pix.py
```

Arquivo gerado:

```text
data/processed/pix_consolidado.csv
```

---

## Etapa 3 — Processamento Batch com PyFlink

Responsável por:

- ingestão do CSV consolidado;
- execução das queries SQL;
- agregações analíticas;
- geração de gráficos.

### Executar:

```bash
python jobs/pix_analysis.py
```

---

# Resultados Gerados

Durante a execução dos Jobs, os resultados serão exibidos de forma geral no terminal, ao mesmo tempo, também serão gerados outputs gráficos em:

```text
outputs/charts/
```

Os arquivos gerados serão:

| Arquivo | Descrição |
|---|---|
| evolucao_pix.png | Evolução temporal do volume financeiro |
| natureza_temporal.png | Evolução financeira por natureza |
| regioes_pix.png | Comparação financeira entre regiões |

---

# Execução do Streaming Simulado

O projeto inclui um exemplo simplificado de processamento streaming utilizando a DataStream API para demonstração.

## Executar:

```bash
python jobs/pix_stream_job.py
```

---

# Objetivo do Streaming

Demonstrar:

- conceitos básicos de stream processing;
- fluxo contínuo de eventos;
- DataStream API;
- emissão incremental de registros.

---

# Fluxo de Processamento Técnico

## 1. Ingestão

Os datasets são carregados via:

- Pandas;
- CSV;
- PyFlink Table API.

---

## 2. Transformação

Transformações realizadas:

- agrupamentos;
- filtros;
- agregações;
- ordenações;
- consolidações temporais.

---

## 3. Processamento Batch

O ambiente batch é inicializado utilizando:

```python
EnvironmentSettings.in_batch_mode()
```

Permitindo:

- consultas SQL;
- agregações históricas;
- analytics em lote.

---

## 4. Agregações

Exemplos utilizados:

```sql
SUM(VALOR)
SUM(QUANTIDADE)

GROUP BY AnoMes
GROUP BY NATUREZA
GROUP BY PAG_REGIAO
```

---

## 5. Visualização

Após o processamento:

1. Os resultados do Flink são coletados;
2. Convertidos para DataFrames Pandas;
3. Exportados em gráficos PNG utilizando Matplotlib.

---

# Datasets Utilizados

## Fonte Oficial

Banco Central do Brasil:

```text
https://dadosabertos.bcb.gov.br/dataset/pix
```

---

## Estrutura dos Dados

Campos principais utilizados:

| Campo | Descrição |
|---|---|
| AnoMes | Referência temporal |
| VALOR | Valor financeiro |
| QUANTIDADE | Quantidade de transações |
| NATUREZA | Natureza da transação |
| PAG_REGIAO | Região pagadora |

---

## Problemas Conhecidos

Possíveis inconsistências encontradas:

- valores nulos;
- schemas diferentes entre períodos;
- encoding inconsistente;
- arquivos incompletos;
- mudanças estruturais no dataset oficial.

---

# Troubleshooting

## Erro: ModuleNotFoundError

### Causa

Dependência ausente no ambiente virtual.

### Solução

```bash
pip install -r requirements.txt
```

---

## Erro: Java Gateway Process Exited

### Causa

Java ausente ou incompatível.

### Solução

Instalar Java 11+ e configurar:

```text
JAVA_HOME
```

---

## Erro: FileNotFoundError

### Causa

CSV não encontrado.

### Solução

Executar:

```bash
python scripts/pix_downloader.py
```

e depois:

```bash
python scripts/normalizar_pix.py
```

---

## Erro: Pandas não encontrado

### Causa

Ambiente virtual não ativado.

### Solução

### Windows

```powershell
.venv\Scripts\activate
```

### Linux/macOS

```bash
source .venv/bin/activate
```

---

## Problemas de Encoding

### Causa

Arquivos CSV em encoding incompatível.

### Solução

Utilizar:

```python
encoding="utf-8"
```

---

## Problemas com Paths

### Causa

Execução a partir de diretório incorreto.

### Solução

Executar os scripts sempre a partir da raiz do projeto:

```bash
cd flink-testing
```

---

# Melhorias Futuras (Em consideração)

Possíveis evoluções do projeto:

- processamento streaming real;
- deploy em cluster Flink;
- persistência em banco analítico;
- monitoramento em tempo real e/ou faixas de tempo.

---

# Conceitos Demonstrados

Este projeto cobre:

- **Processamento Batch** — execução de jobs PyFlink em modo batch sobre arquivos históricos com `in_batch_mode()`
- **Conceitos de Streaming** — uso do modo streaming para observar atualização incremental de contagens em tempo real
- **Analíticas de SQL** — queries com `GROUP BY`, `ORDER BY`, `LIMIT`, `SUM`, `AVG` e `ROUND` via Table API
- **PyFlink Table API** — definição de source/sink com `TableDescriptor`, schema tipado e `FormatDescriptor`
- **Visualização de Dados** — geração de gráficos a partir dos resultados dos jobs
- **Pipelines Analíticas** — separação clara entre download, normalização e análise em scripts independentes
