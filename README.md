<p align="center">
  
  <h2 align="center">POC</h2>
  <h3 align="center">Mensagens instantâneas das chamadas para bolsas do IPEA</h3>
  
</p>

<br>

## Objetivo Geral
- Implementar uma POC de sistema de envio automático de mensagens instantâneas informando sobre novos editais puplicados, a fim de validar a hipótese que essa ferramenta pode ser criada



## Ferramentas
- Python
- BeatifulSoup
- Parquet
- Logging




```plaintext
.
├── data/
│   ├── chamadas_bolsas_ipea_bronze.parquet # arquivo com os dados coletados do webscrapper, apenas com formatação minima e salvo em parquet
│   ├── chamadas_bolsas_ipea_silver.parquet # arquivo com dados tratados e enriquecidos em relação a camada bronze
│   ├── chamadas_bolsas_ipea_gold.parquet   # arquivo com dados tratados e enriquecidos em relação a camada gold (esse que é usado pelo bot)
│   └── usuarios_bot.json                   # arquivo com todos os ids e indicativo de disparo de alerta
│
├── scripts/                 
│   ├── webscrapper_editais.py              # web scrapper que coleta as informações de interesse dos editais e armazena essas informações
│   ├── tratamento_dados.py                 # script de tratamento dos dados coletados pelo webscrapper, fazendo alusão as tratamento referentes aos níveis das camadas
│   └── bot_editais.py                      # bot no telegram para envio de alertas, gerenciamento de usuários e obtenção de algumas informações rápidas dos editais abertos   
│
├── credenciais.json                        # arquivo com todas as credenciais necessárias, atualmente só tem o token de telegram
└── README.md             
```