<p align="center">
  
  <h2 align="center">POC</h2>
  <h3 align="center">Mensagens instantâneas das chamadas para bolsas do IPEA</h3>
  
</p>

<br>

## Objetivo Geral
- Implementar um sistema de envio automático de mensagens instantâneas informando sobre novos editais puplicados.

## Ferramentas
- Python
- BeatifulSoup
- Parquet
- Logging




```plaintext
.
├── data/
│   ├── chamadas_bolsas_ipea_bronze.parquet
│   ├── chamadas_bolsas_ipea_silver.parquet
│   ├── chamadas_bolsas_ipea_gold.parquet
│   └── usuarios_bot.json  
│
├── scripts/                 
│   ├── webscrapper_editais.py
│   ├── tratamento_dados.py
│   └── bot_editais.py     
│
├── credenciais.json
└── README.md             
```