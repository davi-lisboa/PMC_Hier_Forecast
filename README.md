# PMC Hierarchical Forecast

Este projeto é um pipeline analítico focado na extração, tratamento e previsão (forecast) de séries temporais das atividades da Pesquisa Mensal de Comércio (PMC) do IBGE. Ele propõe a utilização de técnicas de previsões hierárquicas, reconciliando previsões nos níveis mais agregados com os níveis desagregados.

## Objetivos e Proposta

- **Automatização de Coleta**: Busca histórica do volume de vendas do comércio usando as APIs do IBGE via `sidrapy`.
- **Padronização e Tratamento**: Organiza as atividades em níveis hierárquicos consistentes e aplica os pesos oficiais.
- **Modelagem Hierárquica**: Utiliza o framework `sktime` integrando múltiplos modelos (ARIMA, ETS, CES, TBATS), um metamodelo com LightGBM e uma técnica de reconciliação ótima (`OptimalReconciler`) para gerar consistência entre todos os níveis de hierarquia da PMC.
- **Eficiência Operacional**: Identifica automaticamente se existem dados novos do IBGE. Caso contido dentro da atualização, retreina e produz as métricas YoY, se não, encerra a execução silenciosamente.

## Estrutura Principal

* `pipeline/coleta.py`: Funções responsáveis pela requisição via `sidrapy` e leitura de pesos base. Usa tratativas de retry para resiliência de rede.
* `pipeline/tratamento.py`: Lógica para transformar estruturas hierárquicas da classificação original em agrupamentos úteis.
* `pipeline/modelo.py`: Fábrica do modelo univariado/hierárquico e manipuladores de estado (save/load bundle para persistir os modelos na pasta data).
* `pipeline/reports.py`: Funções auxiliares para gerar resumos descritivos de performance e avaliações YoY (year-over-year).
* `pipeline/run_pipeline.py`: Ponto de entrada (entrypoint) que orquestra todo o fluxo, conectando as dependências, executando as checagens e realizando a inferência final.
