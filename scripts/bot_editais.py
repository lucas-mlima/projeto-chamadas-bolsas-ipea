# -*- coding: utf-8 -*-


import pandas as pd
import json
import os
import logging
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, Application
from datetime import datetime
import time

# --- Configurações --- 
CREDENCIAIS_PATH = "./credenciais.json"
DATAFILE = "./data/chamadas_bolsas_ipea_gold.parquet" 
USER_DB = "./data/usuarios_bot.json" 

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING) # Silenciar logs excessivos do httpx
logger = logging.getLogger(__name__)

# --- Variáveis Globais para Cache Simples --- 
df_cache = None
last_load_time = 0
last_mod_time = 0
CACHE_DURATION = 60 # Segundos - Recarrega o Parquet se mais antigo que isso ou se modificado


# --- Funções Auxiliares --- 

def carregar_config(path=CREDENCIAIS_PATH):
    """Carrega o token do arquivo JSON."""
    try:
        with open(path, "r", encoding='utf-8') as f:
            config = json.load(f)
            token = config.get("TELEGRAM_TOKEN")
            if not token or token == "SEU_TOKEN_AQUI":
                logger.error(f"Token do Telegram não encontrado ou não configurado em {path}")
                return None
            return token
    except FileNotFoundError:
        logger.error(f"Arquivo de credenciais não encontrado em {path}")
        return None
    except (json.JSONDecodeError, Exception) as e:
        logger.error(f"Erro ao ler ou processar {path}: {e}")
        return None

def load_data_with_cache(filepath=DATAFILE):
    """Carrega dados do Parquet com cache simples baseado no tempo de modificação."""
    global df_cache, last_load_time, last_mod_time
    try:
        current_mod_time = os.path.getmtime(filepath)
        now = time.time()

        # Recarrega se o cache expirou, o arquivo foi modificado, ou o cache está vazio
        if df_cache is None or (now - last_load_time > CACHE_DURATION) or (current_mod_time > last_mod_time):
            logger.info(f"Recarregando dados de {filepath}...")
            df = pd.read_parquet(filepath)
            
            # --- Validações e Pré-processamento --- 
            required_cols = ['numero_chamada', 'ano_chamada', 'link_chamada', 'dt_fim', 'edital_aberto', 'horas_restantes']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Colunas faltando no Parquet: {missing_cols}")
                # Poderia retornar None ou um DataFrame vazio, dependendo da estratégia
                # Retornando o que tem, mas logando o erro.
                pass # Continua com as colunas que tem

            if 'numero_chamada' in df.columns:
                df['numero_chamada'] = df['numero_chamada'].astype(str)
            if 'ano_chamada' in df.columns:
                 df['ano_chamada'] = df['ano_chamada'].astype(str)
            if 'dt_fim' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['dt_fim']):
                df['dt_fim'] = pd.to_datetime(df['dt_fim'], errors='coerce')
            # Recalcular edital_aberto e horas_restantes para garantir atualização
            if 'dt_fim' in df.columns:
                 now_dt = datetime.now()
                 df['edital_aberto'] = df['dt_fim'].apply(lambda x: 1 if pd.notna(x) and x >= now_dt else 0)
                 df['horas_restantes'] = df['dt_fim'].apply(lambda x: (x - now_dt).total_seconds() / 3600 if pd.notna(x) and x >= now_dt else 0.0)
                 df['horas_restantes'] = df['horas_restantes'].clip(lower=0)
            # --- Fim Validações --- 

            df_cache = df
            last_load_time = now
            last_mod_time = current_mod_time
            logger.info(f"Cache atualizado. {len(df_cache)} linhas carregadas.")
        else:
            logger.debug("Usando dados do cache.")
        return df_cache.copy() # Retorna cópia para evitar modificação acidental do cache

    except FileNotFoundError:
        logger.error(f"Arquivo Parquet não encontrado em {filepath}")
        df_cache = None # Invalida cache se arquivo sumir
        return None
    except Exception as e:
        logger.error(f"Erro ao carregar ou processar o arquivo Parquet: {e}")
        # Não invalida o cache necessariamente, pode ser erro temporário
        return None

def load_users():
    """Carrega usuários do JSON (agora como dicionário)."""
    if not os.path.exists(USER_DB):
        return {}
    try:
        with open(USER_DB, "r", encoding='utf-8') as f:
            # Garante que as chaves sejam strings (IDs de usuário)
            data = json.load(f)
            return {str(k): v for k, v in data.items()} 
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Erro ao carregar JSON de usuários {USER_DB}: {e}. Retornando vazio.")
        return {}

def save_users(users_dict):
    """Salva usuários no JSON (como dicionário)."""
    try:
        with open(USER_DB, "w", encoding='utf-8') as f:
            json.dump(users_dict, f, ensure_ascii=False, indent=4)
    except IOError as e:
        logger.error(f"Erro ao salvar JSON de usuários {USER_DB}: {e}")

def format_remaining_time(total_hours):
    """Formata horas em dias e horas de forma mais legível."""
    if total_hours <= 0:
        return "Encerrado"
    try:
        total_hours = float(total_hours)
        delta = datetime.timedelta(hours=total_hours)
        days = delta.days
        seconds_rem = delta.seconds
        hours = seconds_rem // 3600
        # minutes = (seconds_rem % 3600) // 60 # Se quiser minutos
        
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        # if minutes > 0:
        #     parts.append(f"{minutes}m")
        
        if not parts: # Menos de 1 hora
             return "< 1h"
        return " ".join(parts)
    except Exception as e:
        logger.warning(f"Erro ao formatar horas {total_hours}: {e}")
        return "Erro"

# --- Handlers dos Comandos --- 

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    usuarios = load_users()
    
    if user_id not in usuarios:
        usuarios[user_id] = True # Adiciona e marca como ativo por padrão
        save_users(usuarios)
        reply_text = f"Olá {user.mention_html()}! ✅ Você foi adicionado e receberá alertas de novos editais.\nUse /stop para parar ou /ajuda para ver comandos."
    else:
        reply_text = f"Olá {user.mention_html()}! Você já está na lista. Use /ajuda para comandos."
        if not usuarios.get(user_id, False): # Se estava na lista mas inativo
             usuarios[user_id] = True
             save_users(usuarios)
             reply_text += "\nSeus alertas foram reativados!"
             
    await update.message.reply_html(reply_text)

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    usuarios = load_users()
    if user_id in usuarios and usuarios[user_id]:
        usuarios[user_id] = False # Marca como inativo em vez de remover
        save_users(usuarios)
        await update.message.reply_text("❌ Alertas desativados. Você não receberá mais notificações.")
    else:
        await update.message.reply_text("ℹ️ Você já não estava recebendo alertas.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envia a lista de comandos quando /ajuda é emitido."""
    help_text = (
        "Comandos disponíveis:\n"
        "/start - Ativa/Reativa o recebimento de alertas.\n"
        "/stop - Desativa o recebimento de alertas.\n"
        "/ajuda - Mostra esta mensagem.\n"
        "/mais_recente - Mostra o número do edital mais recente.\n"
        "/link <numero> - Obtém o link do edital (ex: /link 33).\n"
        "/abertos - Lista os editais com inscrições abertas.\n\n"
        "*Nota:* A verificação de novos editais precisa é feita externamente por outro componente da solução."
    )
    await update.message.reply_text(help_text)

async def mais_recente(update: Update, context: ContextTypes.DEFAULT_TYPE):
    df = load_data_with_cache()
    if df is None or df.empty:
        await update.message.reply_text("Desculpe, não consegui carregar os dados dos editais agora.")
        return

    try:
        # Garante que as colunas existem antes de ordenar
        if 'ano_chamada' not in df.columns or 'numero_chamada' not in df.columns:
             await update.message.reply_text("Erro: Colunas 'ano_chamada' ou 'numero_chamada' não encontradas nos dados.")
             return
             
        # Ordenar por ano (desc) e depois número (desc)
        # Convertendo numero_chamada para numérico para ordenação correta, se possível
        df['num_int'] = pd.to_numeric(df['numero_chamada'], errors='coerce').fillna(0)
        df_sorted = df.sort_values(by=['ano_chamada', 'num_int'], ascending=[False, False])
        
        if df_sorted.empty:
             await update.message.reply_text("Não há editais nos dados carregados.")
             return
             
        latest = df_sorted.iloc[0]
        await update.message.reply_text(f"📌 Edital mais recente: {latest['numero_chamada']}/{latest['ano_chamada']}")
        
    except KeyError as e:
        logger.error(f"KeyError em /mais_recente: {e}")
        await update.message.reply_text(f"Erro ao processar os dados (coluna {e} faltando?).")
    except Exception as e:
        logger.error(f"Erro inesperado em /mais_recente: {e}")
        await update.message.reply_text("Ocorreu um erro ao buscar o edital mais recente.")

async def link_por_numero(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❗ Uso: /link <numero_do_edital>")
        return
    numero_query = context.args[0]

    df = load_data_with_cache()
    if df is None or df.empty:
        await update.message.reply_text("Desculpe, não consegui carregar os dados dos editais agora.")
        return

    try:
        if 'numero_chamada' not in df.columns or 'link_chamada' not in df.columns:
             await update.message.reply_text("Erro: Colunas 'numero_chamada' ou 'link_chamada' não encontradas.")
             return
             
        # Busca pelo número (comparando como string)
        edital = df[df['numero_chamada'] == numero_query]
        
        if edital.empty:
            await update.message.reply_text(f"🚫 Edital nº {numero_query} não encontrado.")
        else:
            url = edital.iloc[0]['link_chamada']
            await update.message.reply_text(f"🔗 Link do edital {numero_query}: {url}")
            
    except KeyError as e:
        logger.error(f"KeyError em /link: {e}")
        await update.message.reply_text(f"Erro ao processar os dados (coluna {e} faltando?).")
    except Exception as e:
        logger.error(f"Erro inesperado em /link para {numero_query}: {e}")
        await update.message.reply_text("Ocorreu um erro ao buscar o link.")

async def editais_abertos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    df = load_data_with_cache()
    if df is None or df.empty:
        await update.message.reply_text("Desculpe, não consegui carregar os dados dos editais agora.")
        return

    try:
        if 'edital_aberto' not in df.columns or 'dt_fim' not in df.columns:
             await update.message.reply_text("Erro: Colunas 'edital_aberto' ou 'dt_fim' não encontradas.")
             return
             
        # Filtrar usando a coluna pré-calculada
        abertos = df[df['edital_aberto'] == 1].copy()
        
        if abertos.empty:
            await update.message.reply_text("✅ Nenhum edital aberto encontrado no momento.")
            return

        # Ordenar por data de fim mais próxima
        abertos_sorted = abertos.sort_values('dt_fim', ascending=True)

        mensagens = ["*Editais Abertos:*\n"] # Usando Markdown V2 requer escape
        for _, row in abertos_sorted.iterrows():
            num = row.get('numero_chamada', '?')
            ano = row.get('ano_chamada', '?')
            link = row.get('link_chamada', 'N/A')
            horas_rest = round(row.get('horas_restantes', 0),2)
            
            # Escape para MarkdownV2 se necessário, ou usar HTML parse_mode
            resumo = f"📌 Edital *{num}/{ano}*\n⏳ Restam: *{horas_rest}* horas\n🔗 Link: {link}"
            mensagens.append(resumo)

        mensagem_final = "\n\n".join(mensagens)
        # Limitar tamanho da mensagem se necessário (Telegram tem limite)
        if len(mensagem_final) > 4000: # Limite aproximado
             mensagem_final = mensagem_final[:4000] + "... (lista muito longa)"
             
        await update.message.reply_text(mensagem_final, parse_mode='Markdown') # Usar Markdown ou HTML

    except KeyError as e:
        logger.error(f"KeyError em /abertos: {e}")
        await update.message.reply_text(f"Erro ao processar os dados (coluna {e} faltando?).")
    except Exception as e:
        logger.error(f"Erro inesperado em /abertos: {e}")
        await update.message.reply_text("Ocorreu um erro ao listar os editais abertos.")



# --- Função para Alerta (Externa) --- 
# Mantida como no original, mas com logging e usando o token carregado
def enviar_alerta_novos_editais(novos_editais, telegram_token):
    """Envia alerta para usuários ativos. Deve ser chamada externamente."""
    if not novos_editais:
        logger.info("Nenhum novo edital fornecido para alerta.")
        return
        
    usuarios = load_users()
    active_users_ids = [user_id for user_id, active in usuarios.items() if active]
    
    if not active_users_ids:
        logger.info("Nenhum usuário ativo para receber alertas.")
        return
        
    if not telegram_token:
         logger.error("Token do Telegram não disponível para enviar alertas.")
         return
         
    bot = Bot(token=telegram_token)
    logger.info(f"Enviando alertas sobre {len(novos_editais)} edital(is) para {len(active_users_ids)} usuário(s)...")
    
    for user_id in active_users_ids:
        try:
            for edital in novos_editais: # Envia uma msg por edital?
                num = edital.get('numero_chamada', '?')
                ano = edital.get('ano_chamada', '?')
                link = edital.get('link_chamada', 'N/A')
                msg = f"📢 Novo edital publicado: *{num}/{ano}*\n🔗 Link: {link}"
                # Usar asyncio para enviar mensagens pode ser mais eficiente se muitos usuários/mensagens
                # Mas para simplicidade, chamadas síncronas aqui (requer `pip install nest_asyncio` se chamado de contexto async)
                # Ou melhor, adaptar para ser chamada de dentro do loop async do bot se for integrado
                bot.send_message(chat_id=user_id, text=msg, parse_mode='Markdown')
            logger.debug(f"Alerta(s) enviado(s) para {user_id}")
        except Exception as e:
            logger.error(f"Falha ao enviar alerta para {user_id}: {e}")
            # Considerar marcar usuário como inativo se erro persistir?



# --- Função Principal do Bot --- 

def main():
    """Inicia o bot e configura os handlers."""
    TELEGRAM_TOKEN = carregar_config()
    if not TELEGRAM_TOKEN:
        logger.critical("Falha ao carregar o token do Telegram. Encerrando.")
        return

    # Cria a Application
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Registra os handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("ajuda", help_command))
    application.add_handler(CommandHandler("mais_recente", mais_recente))
    application.add_handler(CommandHandler("link", link_por_numero))
    application.add_handler(CommandHandler("abertos", editais_abertos))
    
    # Adicionar outros handlers se necessário (e.g., para /verificar_novos se integrado)

    # Inicia o Bot
    logger.info("Iniciando o bot...")
    application.run_polling()
    logger.info("Bot encerrado.") # Só será logado se run_polling parar

if __name__ == "__main__":
    main()

