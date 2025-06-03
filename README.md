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
- Telegram




```plaintext
.
├── data/
│   ├── chamadas_bolsas_ipea_bronze.parquet # Dados coletados do webscrapper (camada bronze)
│   │
│   ├── chamadas_bolsas_ipea_silver.parquet # Dados tratados e enriquecidos (camada silver)
│   │
│   ├── chamadas_bolsas_ipea_gold.parquet   # Dados tratados e enriquecidos (camada gold)
│   │
│   └── usuarios_bot.json                   # Arquivo informações de usuário
│
├── scripts/                 
│   ├── webscrapper_editais.py              # Web scrapper que coleta e armazenamento das chamadas
│   │ 
│   ├── tratamento_dados.py                 # Tratamento da camada bronze a gold
│   │
│   └── bot_editais.py                      # Bot no Telegram com envios de alertas e   
│                                           # algumas features
│
├── credenciais.json                        # arquivo com todas as credenciais necessárias, 
│                                           
└── README.md             
```