import os
import json
import logging
import asyncio
import re
import hashlib
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
from newsapi import NewsApiClient
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
from newspaper import Article, Config

# --- CONFIGURAZIONE ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

TOKEN = os.getenv("TELEGRAM_TOKEN")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
INPUT_TOPIC = os.getenv("INPUT_TOPIC")
ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")

# --- CLIENT ---
newsapi = NewsApiClient(api_key=NEWS_API_KEY)
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
es = Elasticsearch(hosts=[ES_HOST])

WEIGHT_ML = 0.6
WEIGHT_USER = 0.4

# --- HELPER FUNCTIONS ---

def generate_article_id(url):
    clean_url = url.split('?')[0]
    return hashlib.md5(clean_url.encode('utf-8')).hexdigest()

def article_exists(article_id):
    """
    Controlla se l'articolo esiste già usando una query di conteggio.
    È più robusto di es.exists() quando si usano wildcard negli indici.
    """
    try:
        # Usiamo 'count' invece di 'exists'.
        # Cerchiamo tra TUTTI gli indici che iniziano con 'articles-analysis-'
        # se c'è un documento con questo specifico _id.
        query = {
            "query": {
                "ids": {
                    "values": [article_id]
                }
            }
        }
        res = es.count(index="articles-analysis-*", body=query)

        # Se il conteggio è > 0, l'articolo esiste già
        return res['count'] > 0
    except Exception as e:
        logging.warning(f"Errore controllo esistenza articolo: {e}")
        return False

def normalize_name(name):
    return re.sub(r'\s+', '_', name.strip().lower())

def scrape_article(url):
    """Scarica il testo dell'articolo. Timeout breve per non bloccare il bot."""
    try:
        conf = Config()
        conf.browser_user_agent = 'Mozilla/5.0'
        conf.request_timeout = 4 # 4 secondi max
        conf.fetch_images = False

        article = Article(url, config=conf)
        article.download()
        article.parse()

        text = article.text
        # Prendiamo max 2000 caratteri per non intasare Kafka
        return text[:2000] if text else ""
    except Exception as e:
        logging.warning(f"Scraping fallito per {url}: {e}")
        return ""

async def update_leaderboard(person_name, score, ml_count, user_count):
    person_id = normalize_name(person_name)
    doc = {
        "person_name": person_name.title(),
        "person_id": person_id,
        "hybrid_score": float(score),
        "ml_count": ml_count,
        "user_count": user_count,
        "last_updated": datetime.now()
    }
    try:
        es.index(index="index-leaderboard", id=person_id, document=doc)
    except Exception as e:
        logging.error(f"Errore leaderboard: {e}")

async def log_search_event(user_id, username, query_text, articles_found_count):
    doc = {
        "user_id": user_id,
        "username": username,
        "query": query_text.lower().strip(),
        "display_query": query_text.strip(),
        "articles_found": articles_found_count,
        "timestamp": datetime.now()
    }
    try:
        es.index(index="user-searches", document=doc)
    except Exception as e:
        logging.error(f"Errore log ricerca: {e}")

async def get_hybrid_score(person_name):
    normalized_name = normalize_name(person_name)

    # 1. ML Score
    ml_score = 0.5
    ml_count = 0
    try:
        ml_query = {
            "size": 0,
            "query": { "match": { "query": person_name } },
            "aggs": { "verdict_counts": { "terms": { "field": "analysis.verdict.keyword" } } }
        }
        res_ml = es.search(index="articles-analysis-*", body=ml_query, ignore=[404])
        if 'aggregations' in res_ml:
            buckets = res_ml['aggregations']['verdict_counts']['buckets']
            duce_hits = next((b['doc_count'] for b in buckets if b['key'] == 'DUCE'), 0)
            non_duce_hits = next((b['doc_count'] for b in buckets if b['key'] == 'NON DUCE'), 0)
            total_ml = duce_hits + non_duce_hits
            if total_ml > 0:
                ml_score = duce_hits / total_ml
                ml_count = total_ml
    except Exception: pass

    # 2. User Score
    user_score = 0.5
    user_count = 0
    try:
        user_query = {
            "size": 0,
            "query": { "term": { "person_id.keyword": normalized_name } },
            "aggs": { "avg_vote": { "avg": { "field": "is_duce_vote" } } }
        }
        res_user = es.search(index="user-feedback", body=user_query, ignore=[404])
        if res_user.get('hits', {}).get('total', {}).get('value', 0) > 0:
            avg_val = res_user['aggregations']['avg_vote']['value']
            if avg_val is not None: user_score = avg_val
            user_count = res_user['hits']['total']['value']
    except Exception: pass

    if ml_count == 0 and user_count == 0: return None, 0, 0

    w_ml = WEIGHT_ML if ml_count > 0 else 0
    w_user = WEIGHT_USER if user_count > 0 else 0
    total_w = w_ml + w_user

    if total_w == 0: return 0.5, 0, 0

    final_score = ((ml_score * w_ml) + (user_score * w_user)) / total_w
    return final_score, ml_count, user_count

# --- COMMAND HANDLERS ---

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = " ".join(context.args).strip()
    if not query:
        await update.message.reply_text("Usa: /check <Nome Cognome>")
        return

    user = update.effective_user
    status_msg = await update.message.reply_text(f"📡 **Analisi avviata per: {query}**\nScarico le notizie e le invio all'AI...", parse_mode='Markdown')

    # --- INGESTIONE + SCRAPING ---
    articles_found = False
    new_articles_sent = 0

    try:
        all_articles = newsapi.get_everything(q=query, language='it', sort_by='relevancy', page_size=10)
        articles = all_articles.get('articles', [])

        if articles:
            articles_found = True

            # Feedback visivo "Sto scaricando..."
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

            for article in articles:
                url = article['url']
                article_id = generate_article_id(url)

                # Deduplicazione
                if article_exists(article_id):
                    continue

                # SCRAPING NEL BOT
                full_text = scrape_article(url)

                payload = {
                    "id": article_id,
                    "query": query,
                    "title": article['title'],
                    "description": article['description'] or "",
                    "url": url,
                    "full_text": full_text,
                    "source": article['source']['name'],
                    "publishedAt": article['publishedAt']
                }
                producer.send(INPUT_TOPIC, value=payload)
                new_articles_sent += 1

            producer.flush()
            logging.info(f"Inviati {new_articles_sent} articoli a Kafka.")

    except Exception as e:
        logging.error(f"Errore NewsAPI: {e}")

    await log_search_event(user.id, user.username, query, len(articles) if 'articles' in locals() else 0)

    # --- PRIMA DEL POLLING: Salviamo quanti articoli c'erano GIA' ---
    _, ml_historical, _ = await get_hybrid_score(query)
    target_count = ml_historical + new_articles_sent

    await log_search_event(user.id, user.username, query, len(articles) if 'articles' in locals() else 0)

    # --- POLLING (Time-based & Stability-based) ---
    timeout_seconds = 600 if new_articles_sent > 0 else 5
    start_time = datetime.now()
    wait_messages = ["🧠 L'AI sta leggendo i testi...", "🐢 Analisi approfondita in corso...", "🔍 Calcolo punteggi..."]

    final_score = None
    ml_c, user_c = 0, 0

    # Variabili per capire se Spark ha smesso di scrivere
    last_ml_count = ml_historical
    stable_cycles = 0

    while True:
        elapsed = (datetime.now() - start_time).total_seconds()
        if elapsed > timeout_seconds:
            break

        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

        score, ml, usr = await get_hybrid_score(query)

        if score is not None:
            if new_articles_sent > 0:
                # STIAMO ASPETTANDO NUOVI ARTICOLI
                if ml >= target_count:
                    # Sono arrivati tutti! Possiamo uscire.
                    final_score, ml_c, user_c = score, ml, usr
                    break
                elif ml > ml_historical:
                    # Ne è arrivato qualcuno, ma forse non tutti. Controlliamo se si è bloccato.
                    if ml == last_ml_count:
                        stable_cycles += 1
                    else:
                        stable_cycles = 0
                        last_ml_count = ml

                    # Se il numero di articoli analizzati non sale per 4 controlli di fila (circa 12 secondi),
                    # diamo per scontato che Spark abbia finito (magari un articolo è andato in errore e non arriverà mai).
                    if stable_cycles >= 4:
                        final_score, ml_c, user_c = score, ml, usr
                        break
            else:
                # NESSUN NUOVO ARTICOLO: usciamo subito con i dati storici
                final_score, ml_c, user_c = score, ml, usr
                break

        # Feedback visivo all'utente
        if articles_found:
            if int(elapsed) > 0 and int(elapsed) % 15 == 0:
                msg_index = (int(elapsed) // 15) % len(wait_messages)
                try:
                    await context.bot.edit_message_text(
                        chat_id=update.effective_chat.id,
                        message_id=status_msg.message_id,
                        text=f"📡 **Analisi in corso per: {query}**\n\n{wait_messages[msg_index]}\n_(Tempo trascorso: {int(elapsed)}s)_",
                        parse_mode='Markdown'
                    )
                except Exception: pass

            await asyncio.sleep(3)
        else:
            break

    # --- RISULTATO ---
    if final_score is not None:
        await update_leaderboard(query, final_score, ml_c, user_c)

        ducismo_perc = round(final_score * 100, 1)
        verdict = "DUCE" if final_score > 0.5 else "NON DUCE"
        emoji = "👮‍♂️" if final_score > 0.5 else "🕊️"

        msg = (
            f"{emoji} **RISULTATO ANALISI: {query.upper()}**\n\n"
            f"📊 **Indice:** {ducismo_perc}%\n"
            f"🤖 Basato su {ml_c} articoli analizzati\n"
            f"🗣️ Basato su {user_c} voti utenti\n\n"
            f"Verdetto: **{verdict}**\n\n"
            f"_Sei d'accordo?_"
        )
        keyboard = [[
            InlineKeyboardButton("✅ Sì", callback_data=f"vote|yes|{query}|{verdict}"),
            InlineKeyboardButton("❌ No", callback_data=f"vote|no|{query}|{verdict}")
        ]]
        await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=status_msg.message_id, text=msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        err_msg = "⏳ Analisi lenta, riprova tra poco." if articles_found else "🤷‍♂️ Dati insufficienti."
        await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=status_msg.message_id, text=err_msg, parse_mode='Markdown')

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split("|")
    vote_type, person_name, sys_verdict = data[1], data[2], data[3]
    user_id = query.from_user.id

    vote_val = 0
    if sys_verdict == "DUCE": vote_val = 1 if vote_type == "yes" else 0
    else: vote_val = 0 if vote_type == "yes" else 1

    person_id = normalize_name(person_name)
    doc = {
        "user_id": user_id, "person_name": person_name, "person_id": person_id,
        "vote_type": vote_type, "is_duce_vote": vote_val, "timestamp": datetime.now()
    }
    try:
        es.create(index="user-feedback", id=f"{user_id}_{person_id}", document=doc)
        rsp = "✅ Voto registrato!"
    except Exception: rsp = "⚠️ Hai già votato!"

    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text(rsp)

if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("check", check_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.run_polling()