import json, math, os, random, re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import logging
import asyncio
import requests
from logging.handlers import TimedRotatingFileHandler
import dotenv

from raw import sql

logger = logging.getLogger('logger')

# Reuse the same session for multiple requests
session = requests.Session()

def setup_logging():
    log_folder = "logs"
    log_filename = "yesman-middleman.log"

    logginglevel = logging.DEBUG
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    logger = logging.getLogger('logger')
    logger.setLevel(logginglevel)

    log_filepath = os.path.join(log_folder, log_filename)
    handler = TimedRotatingFileHandler(log_filepath, when="D", interval=1, backupCount=7)
    handler.setLevel(logginglevel)

    formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - %(funcName)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

async def get_leaderboard():
    try:
        url = str(os.getenv('EUGLBAPI'))
        response = requests.get(url)

        if response.status_code == 200:
            json_data = response.json()

            output_file = "./resources/leaderboard.json"
            with open(output_file, 'w') as file:
                json.dump(json_data, file, indent=2)
            logging.info('Downloaded leaderboard to JSON')
    except Exception as e:
        logger.error(f"{e}")

async def get_username(gameid_url):
    response = session.get(gameid_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        b_tags = soup.find_all('b')
        for b_tag in b_tags:
            return b_tag.text
    return None

async def extract_unix_timestamp(input_string):
    pattern = r'new Date\((\d+\.\d+)\);'
    match = re.search(pattern, input_string)
    return float(match.group(1)) if match else None

async def get_gamehistory(userid):
    try:
        # Generate a random delay between 3 and 6 seconds to make the calls less obvious
        delay_seconds = random.uniform(3, 6)
        await asyncio.sleep(delay_seconds)

        url = str(os.getenv('EUGNETGHAPI'))
        url = f'{url}{userid}'
        response = session.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            game_rows = soup.find_all('tr')

            max_rows_to_process = 10  # Adjust the maximum number of rows as needed

            for i, row in enumerate(game_rows):
                if i >= max_rows_to_process:
                    break

                columns = row.find_all('td')
                if len(columns) == 3:
                    game_name = columns[0].text.strip()
                    if game_name != 'fulda':
                        continue

                    link_element = columns[2].find('a')
                    if link_element is not None and 'href' in link_element.attrs:
                        game_link = link_element['href']
                        username = await get_username(game_link)
                        if username is not None:
                            return str(username)
                    else:
                        continue

        return None
    except Exception as e:
        logger.error(f"{e}")

async def processUsernames():
    try:
        data = await load_json_file('./resources/leaderboard.json')

        if data and 'rows' in data and isinstance(data['rows'], list):
            rows = data["rows"]

            # Combine API request and parsing logic
            modified_rows = [{'position': i + 1,
                              'userid': str(row["id"]).replace('u29_', ''),
                              'username': f"{await get_gamehistory(str(row['id']).replace('u29_', ''))}" or None,
                              'rank': str(row["key"]),
                              'elo': math.ceil(float(row["value"]))
                              }
                             for i, row in enumerate(rows[:500])]

            sql.add_usernames_to_db(modified_rows)
        else:
            logger.warning("Invalid JSON structure or 'rows' list not found.")
    except Exception as e:
        logger.error(f"{e}")

async def load_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)
async def startProcessing():

    # Calculate the time until the desired start time
    now = datetime.now()
    desired_start_time = now.replace(hour=desired_hour, minute=desired_minute, second=0, microsecond=0)

    if now >= desired_start_time:
        # If the desired start time has already passed for today, calculate the start time for the next day
        desired_start_time += timedelta(days=1)

    time_until_start = (desired_start_time - now).total_seconds()

    logger.debug(f"Waiting for {time_until_start} seconds until the desired start time.")
    await asyncio.sleep(time_until_start)

    while True:
        await asyncio.gather(get_leaderboard())
        logger.debug('Downloading Leaderboard...')
        logger.debug('Processing Usernames...')
        await asyncio.gather(processUsernames())
        logger.debug('Processing Finished...')

        sleeptimer = 60 * 60 * 1
        logger.debug(f'Sleeping for {sleeptimer} seconds')

        await asyncio.sleep(sleeptimer)

# Set your desired start time (24-hour format)
desired_hour = 11
desired_minute = 0

# Run the event loop
if __name__ == "__main__":
    dotenv.load_dotenv()
    setup_logging()
    asyncio.run(startProcessing())


