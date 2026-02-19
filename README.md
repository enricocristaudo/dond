# Duce o non Duce?
### Analisi della percezione sociale e del profilo autoritario nelle figure pubbliche.

> *"Ha fatto anche cose buone? L'AI ci dà una risposta scientifica (o quasi)."*

![Project Status](https://img.shields.io/badge/Status-Active-brightgreen) ![Tech](https://img.shields.io/badge/Big_Data-Spark_%7C_Kafka_%7C_ELK-blue) ![Mood](https://img.shields.io/badge/Mood-Satirical_%7C_Social_Investigation-red)

## About
**Duce o non Duce** è un progetto di Technologies for Advenced Programming applicata all'indagine sociale e alla satira. 
In un'epoca dominata dal "Lei non sa chi sono io", l'obiettivo è monitorare in tempo reale il linguaggio dei personaggi pubblici (politici, influencer, chef stellati, allenatori di calcio), utilizzando modelli di **Deep Learning** (Zero-Shot Classification) per rilevare retorica autoritaria, toni perentori e deliri di onnipotenza.

Il sistema non si limita all'opinione fredda dell'AI: integra il giudizio popolare attraverso un sistema di voto democratico su Telegram, creando un **Indice Ibrido** che mette a nudo la differenza tra come i media raccontano un "Leader" e come la gente lo percepisce.

## L'Algoritmo "Ibrido"

Il punteggio finale non è arbitrario, ma è calcolato ponderando l'analisi oggettiva della macchina con la percezione soggettiva del pubblico:

$$
Score_{Finale} = (0.6 \times Score_{AI}) + (0.4 \times Score_{Utenti})
$$

* **Score AI:** Basato sull'analisi semantica del tono degli articoli.
* **Score Utenti:** Basato sulla percentuale di voti "Sì, concordo" ricevuti su Telegram.
* **Verdetto:** Se lo score finale supera **0.5 (50%)**, il soggetto entra ufficialmente nella categoria **DUCE**.

## Installazione e Avvio

### Prerequisiti
* Docker & Docker Compose
* Una API Key di [NewsAPI](https://newsapi.org/)
* Un Token Bot di [Telegram](https://t.me/BotFather)
* Risorse consigliate: Almeno 6GB di RAM dedicati a Docker.

### Setup
1.  **Clona la repository:**
    ```bash
    git clone https://github.com/enricocristaudo/dond.git
    cd dond
    ```

2.  **Configura le variabili d'ambiente:**
    Crea un file `.env` nella directory principale:
    ```env
    TELEGRAM_TOKEN=il_tuo_token_telegram
    NEWS_API_KEY=la_tua_chiave_newsapi
    KAFKA_BROKER=kafka:9092
    INPUT_TOPIC=raw-articles
    OUTPUT_TOPIC=classified-articles
    ES_HOST=http://elasticsearch:9200
    ```

3.  **Avvia la Pipeline:**
    ```bash
    docker-compose up -d --build
    ```
    *(Al primo avvio, Spark scaricherà il modello ML. Potrebbe richiedere qualche minuto).*

## Utilizzo

Apri il tuo bot su Telegram e usa i comandi:

* `/start` - Messaggio di benvenuto.
* `/check [Nome Cognome]` - Avvia l'indagine su un personaggio (es. `/check Mario Draghi` o `/check Fedez`).
* Il bot analizzerà le notizie in tempo reale, ti restituirà l'indice di "Ducismo" e ti chiederà se sei d'accordo, registrando il tuo voto.

## Dashboard Kibana

Il progetto include una dashboard analitica accessibile su `http://localhost:5601` per esplorare i dati:

* **Leaderboard del Ducismo:** Classifica globale aggiornata in tempo reale.
* **La voce dei giornali:** Quali testate tendono a esaltare i comportamenti autoritari.
* **Indice di controversia:** I personaggi su cui AI e Utenti sono in totale disaccordo (effetto "Fanboy").
* **Consenso:** Percentuale di accordo globale tra Macchina e Uomo.

## Tech Stack

* **Linguaggio:** Python 3.9
* **Streaming & Big Data:** Apache Kafka, Apache Spark (Structured Streaming)
* **Machine Learning:** PyTorch, Transformers (HuggingFace DeBERTa)
* **Scraping:** [`newspaper3k`](https://github.com/codelucas/newspaper), [`NewsAPI`](https://newsapi.org/)
* **Storage & Data Viz:** Elasticsearch 8.x, Kibana 8.x, Logstash
* **Bot Framework:** `python-telegram-bot`

---

## ⚠️ Disclaimer Finale

Questo progetto ha una duplice natura: è un esperimento satirico data-driven e uno strumento di indagine sociale.

L'algoritmo si limita a misurare specifiche metriche testuali e pattern linguistici. Non esprime giudizi storici o politici sulle persone reali, né ha alcun intento offensivo o diffamatorio verso i soggetti analizzati (siano essi politici, celebrità o allenatori).

“Duce o non Duce” va interpretato con ironia e spirito critico, come uno specchio (volutamente esagerato) dell'arroganza comunicativa dei nostri tempi.