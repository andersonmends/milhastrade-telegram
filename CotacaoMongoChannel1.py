import configparser
import json
import os
from datetime import datetime
import re
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel
from telethon.sessions import StringSession
from os.path import join
from urllib.parse import quote



# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# Setting configuration values
api_id = config['Telegram']['api_id']
# api_id =  os.environ.get('api_id')
api_hash = config['Telegram']['api_hash']
# api_hash = os.environ.get('api_hash')

api_hash = str(api_hash)

phone = config['Telegram']['phone']
# phone = os.environ.get('phone')
username = config['Telegram']['username']
# username = os.environ.get('username')
channel_url = config['Telegram']['channel_url1']
# channel_url = os.environ.get('channel_url1')

session_string = config['Telegram']['session_string']
# session_string = os.environ.get('session_string')
# Create the client and connect
client = TelegramClient(StringSession(session_string), api_id, api_hash)


async def main(phone):
    await client.start()
    print("Client Created")
    # print(client.session.save())
    # Ensure you're authorized
    if await client.is_user_authorized() == False:
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    me = await client.get_me()

    user_input_channel = channel_url

    if user_input_channel.isdigit():
        entity = PeerChannel(int(user_input_channel))
    else:
        entity = user_input_channel

    print("ID do Grupo:", entity)

    my_channel = await client.get_input_entity(entity)

    # replace with your desired date, for all messages set data before telegram channel
    start_date = datetime(2020, 6, 3, 0, 0)
    # start_date = datetime.now()
    print(start_date)

    offset_id = 0
    limit = 100
    all_messages = []
    total_messages = 0
    total_count_limit = 0

    while True:
        print("Current Offset ID is:", offset_id,
              "; Total Messages:", total_messages)
        history = await client(GetHistoryRequest(
            peer=my_channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break

        messages = history.messages

        # Filtering messages to get only those that match the date criteria
        for message in messages:
            if message.date.date() >= start_date.date():
                message.message = re.sub(
                    r"de .*? até ", "", str(message.message))
                message.message = re.findall(
                    r"(HM|Max|Gol|Azul|Latam|[0-9]+,[0-9]+)", message.message)
                message_dict = {
                    'date': message.date.isoformat(), 'message': message.message}
                all_messages.append(message_dict)
            else:
                break  # We reached the end of the messages that match the criteria, so we can stop the loop

        # cleaning data removing unecessary data
        clean_data = []
        for item in all_messages:
            new_item = {"date": item["date"]}
            message = item["message"]
            i = 0
            while i < len(message):
                name = message[i]
                i += 1
                if name in ["HM", "Max"]:
                    new_item["name"] = name
                elif name in ["Latam", "Gol", "Azul"]:
                    data_array = []
                    while i < len(message) and not message[i] in ["Latam", "Gol", "Azul", "HM", "Max"]:
                        data_array.append(float(message[i].replace(",", ".")))
                        i += 1
                    new_item[name] = data_array
                else:
                    i += 1
            clean_data.append(new_item)

        # structuring data to business requeriment
        ready_data = []
        for item in clean_data:
            new_item = {"date": item["date"], "name": item.get("name")}
            for company in ["Latam", "Gol", "Azul"]:
                values = item.get(company, [])
                if values:
                    max_value = max(values)
                    new_item[company] = max_value
            ready_data.append(new_item)

        offset_id = messages[-1].id
        total_messages = len(ready_data)

        if total_count_limit != 0 and total_messages >= total_count_limit:
            break
 
    # Create a new client and connect to the server

    mongo_url = config['Mongo']['mongo_url']
    # mongo_url = os.environ.get('mongo_url')

    clientDB = MongoClient(mongo_url, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        clientDB.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    db = clientDB["cotacao"]
    collection = db["t"]

    collection.insert_many(ready_data)


with client:
    client.loop.run_until_complete(main(phone))
