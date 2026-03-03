# F1 Lake
Coleta, Armazenamento e Processamento de dados de Formula 1 para análise e modelos preditivos - Baseado no projeto do <a href='https://github.com/TeoMeWhy/f1-lake'> Téo me Why </a>

# FastF1

Para o desenvolvimento do projeto será utilizado o pacote FastF1 que dá fácil acesso aos resultados, agendas, dados de tempo e telemetria, resultados de sessões, entre outras informações relacionadas a Formúla 1.

Os dados são providenciados em DataFrames no Pandas e tem suporte ao API da <a href='https://github.com/jolpica/jolpica-f1/blob/main/docs/README.md'> jolpica-F1 </a>, dando acesso a dados atuais e históricos.

mais informações sobre pode ser encontrado na página oficial do <a href="https://docs.fastf1.dev/index.html"> FastF1 </a> 

## Etapas do projeto

### Coleta

Nossa fonte de dados será o [FastF1](https://docs.fastf1.dev/) e a coleta será feita através de scripts de Python.

Ela acontecerá em servidor próprio de maneira agendada.

### Envio dos dados

Os dados também serão enviados a um Bucket S3 na AWS. Vai ser utilizado a Nekt para ter acesso aos dados brutos para realizar ingestão dentro de um Lakehouse.

Aqui será nossa camada `raw`.

### Camada Bronze

Na camada bronze teremos os dados consolidados em formato `Delta` com histórico de modificações, facilitando sua consulta. 

Além disso, teremos uma representação fiel de como este dado poderia ser encontrado em sua origem.

### Camada Silver

A partir dos dados na camada Bronze, dentro do Lakehouse, podemos realizar novas modelangens de dados e também criar **Feature Stores** com o histórico de cada entidade de nosso interesse.

### Camada Gold

Nessa camada ficaram as tabelas em formato de relatório e dados sumarizados, com objetivo de facilitar análise conectados em ferramentas de BI/Dashboards

### Treinamento do Modelo

Momento que será feita uma Analytical Base  Table (ABT) e treinar algoritmos de Machine Learning através de informações tiradas da nossa feature store e conforme nosso interesse de análise. 

Os modelos serão treinados e comparados localmente, através do MLFlow hospedado no servidor.

### Aplicação para usuário

Com nosso modelo treinado, podemos criar uma aplicação onde entusiastas de Fórmula 1 poderão acompanhar as predições do modelo.