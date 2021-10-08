from fastapi import FastAPI, BackgroundTasks
from datetime import datetime
import requests, json
import asyncio, time
import pandas as pd
import redis
import logging, os
import telebot


FIELDS = ['labels.alertname',
          'labels.severity'
          'startsAt',
          'receivers',
          'annotations.description',
          'annotations.runbook_url']
SEVERITY_LVL = ['disaster', 'critical', 'warning']
RECEIVER_LIST = ['default']

logger = logging.getLogger('DefaultLogger')
logger_config = {}


def logger_setup(config_file):
    """logger initialization function.
   Args:
       config_file (dict): dict with parameters for logger.
   Returns:
       logger instance.
   """
    file = config_file.get('LOG_FILENAME')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(filename=file, format='[%(asctime)s] %(levelname)s - %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    return logger

async def scrape_alerts(POLL_INTERVAL, REDIS_SERVER = os.environ["REDIS_SERVER"],
                        REDIS_PORT = int(os.environ["REDIS_PORT"]), ):
    while True:
        try:
            logger.info(f'Scrape alerts from Alert-Manager URL: {os.environ["PROMETHEUS_URL"]}')
            res = requests.get(os.environ["PROMETHEUS_URL"] + "/api/v1/alerts", )
            if res.status_code == 200:
                logger.info(f'Successfully collected data from: {os.environ["PROMETHEUS_URL"]} ')
        except Exception as e:
            logger.exception(f'URL: {os.environ["PROMETHEUS_URL"]} is unreachable')

        payload = json.loads(res.content)

        logger.debug(f'Payload: {payload}')

        alerts = payload["data"]

        df = pd.json_normalize(alerts)

        logger.info(f'Connecting to the redis instance {REDIS_SERVER}:{REDIS_PORT}')

        try:
            r = redis.StrictRedis(host=REDIS_SERVER, port=REDIS_PORT, db=0)
            if r.ping() == True:
                logger.info(f'Succesfully connected to {REDIS_SERVER}:{REDIS_PORT}')
        except redis.ConnectionError as e:
            logger.error(f'Connection error')
            logger.error(e)
        processed_alerts = []
        for i in df.index:
            if r.get(df["fingerprint"].loc[i]) == None:
                if df["receivers"].loc[i][0] in RECEIVER_LIST:
                    msg = ( f'''[FIRING] {df["labels.alertname"].loc[i]} \n\nACTIVE FROM: {datetime.strptime(df["startsAt"].loc[i], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%d-%m-%Y %H:%M')} \n\nSEVERITY: {df["labels.severity"].loc[i].upper()}\n\nDESCRIPTION: {df["annotations.description"].loc[i]}''')
                    try:
                        if df["labels.severity"].loc[i] in SEVERITY_LVL:
                            tb.send_message(os.environ["CHAT_ID"], msg)
                            logger.info(f'Alert with fingerprint {df["fingerprint"].loc[i]} sent to the telegram chat')
                            processed_alerts.append(df["fingerprint"].loc[i])
                            time.sleep(1)
                            r.setex(df["fingerprint"].loc[i], 43200000,
                                    df[df.columns[df.columns.isin(FIELDS)]].loc[i].to_json())
                    except Exception as e:
                        logger.error(f'Telegram alert sending error: {e}')
        if len(processed_alerts) == 0:
            logger.info(f'No new alerts in Alert-Manager {os.environ["PROMETHEUS_URL"]} endpoint')
        logger.info('Collected data, waiting 60 sec...')
        await asyncio.sleep(POLL_INTERVAL)
    return

def start_polling(background_tasks: BackgroundTasks,
                  POLL_INTERVAL=int(os.environ["POLL_INTERVAL"])):
    if POLL_INTERVAL:
        background_tasks.add_task(scrape_alerts, POLL_INTERVAL)
    return

tb = telebot.TeleBot(os.environ["TELEGRAM_TOKEN"])
logger = logger_setup(logger_config)
app = FastAPI()
logger.setLevel(logging.INFO)


@app.get("/start")
async def startup_polling(background_tasks: BackgroundTasks,
                          POLL_INTERVAL=int(os.environ["POLL_INTERVAL"])):
    background_tasks.add_task(scrape_alerts, POLL_INTERVAL)
    time.sleep(1)
    return "OK"

@app.get("/healthz")
def healthz():
    return "OK"


@app.get("/")
async def home():
    return ''' <h1>Alerts service</h1>
   <p>API for getting alerts.</p>'''

@app.get("/alerts/{fingerprint}")
async def get_alert(fingerprint: str,
                    REDIS_SERVER = os.environ["REDIS_SERVER"],
                    REDIS_PORT = int(os.environ["REDIS_PORT"])):
    try:
        r = redis.StrictRedis(host=REDIS_SERVER, port=REDIS_PORT, db=0)
        if r.ping() == True:
            result = r.get(fingerprint)
    except redis.ConnectionError as e:
        logger.error(f'Connection error')
        logger.error(e)
    return {result}