<p align="center">
  
  <h2 align="center">POC</h2>
  <h3 align="center">Mensagens instantâneas das chamadas para bolsas do IPEA</h3>
  
</p>

<br>

## Objetivo Geral
- Implementar uma POC de sistema de envio automático de mensagens instantâneas informando sobre novos editais puplicados, a fim de validar a hipótese que essa ferramenta pode ser criada


## Resultados Preliminares
- POC valida a hipótese que podemos implementar um bot de envio automático de mensagens instantâneas com os novos editais de bolsas do IPEA


## Como usar: passo a passo

1. Acessar sua conta pessoal no telegram;
2. No 'buscar' do telegram, procurar por 'AlertaBolsasIpea_bot' que tem como contato 'AlertaBI_bot' e clicar no contato;
3. Clicar no start que aparecerá na parte inferior da tela. A partir desse momento, o bot já considerará seu contato para fins de envio de alertas;
4. Caso queira saber as outras funcionalidades do bot, mandar /help que você receberá uma mensagem com todos os comandos disponíveis;
5. Caso queira tirar seu usuário do recebimento dos alertas, só mandar /stop (esse comando só retira os envios, o usuário poderá interagir com o bot normalmente).


## Comandos disponíveis:
- /start - Ativa/Reativa o recebimento de alertas.
- /stop - Desativa o recebimento de alertas.
- /ajuda - Mostra esta mensagem.
- /mais_recente - Mostra o número do edital mais recente.
- /link <numero> - Obtém o link do edital (ex: /link 33).
- /abertos - Lista os editais com inscrições abertas.

## Avisos de uso

- O Bot não está otimizado, ainda apresenta gargalos de performance nas respostas aos comandos, principalmente o tempo de resposta. Caso ocorrá demora em reponder algum comando, mandar um segunda vez para forçar a fila andar, depois do segundo envio só esperar;
- Informações serão atualizadas a cada 6 horas. 


## Avisos POC

- O Bot é uma POC, então foi feito considerando o esforço e recursos minímios para sua implementação, por isso algumas decisões foram tomadas desconsiderando o que seria impeditivo ou apenas por performance, como:
  - Não temos um servidor para hospedar nosso bot, esse bot roda localmente, que não é uma solução robusta;
  - Usamos arquivo .json para controlar usuários, o ideal seria um banco de dados Redis, considerando que poderemos ter centenas ou milhares de usuários simultâneos;
  - Não temos encapsulamento da solução como um todo.


## Estrutura do projeto

```plaintext
.
├── data/
│   ├── chamadas_bolsas_ipea_bronze.parquet # Dados coletados do web scraper (camada bronze)
│   ├── chamadas_bolsas_ipea_silver.parquet # Dados tratados e enriquecidos (camada silver)
│   ├── chamadas_bolsas_ipea_gold.parquet   # Dados tratados e enriquecidos (camada gold)
│   └── usuarios_bot.json                   # Arquivo informações de usuário
│
├── scripts/                 
│   ├── webscraper_editais.py               # Web scraper que coleta e armazenamento das chamadas
│   ├── tratamento_dados.py                 # Tratamento da camada bronze a gold
│   ├── bot_editais.py                      # Bot no Telegram com envios de alertas e algumas features
│   └── run_update.py                       # Rodar periodicamente a coleta e tratamento
│
│
├── credenciais.json                        # arquivo com todas as credenciais necessárias 
│                                           
└── README.md             
```