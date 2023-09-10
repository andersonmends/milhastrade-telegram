import configparser
import json
import asyncio
from datetime import datetime, timedelta
import re

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel

# extract messages from telegram channel from date X

# some functions to parse json date
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)

# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# Setting configuration values
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']

api_hash = str(api_hash)

phone = config['Telegram']['phone']
username = config['Telegram']['username']
channel_url = config['Telegram']["channel_url2"]

# Create the client and connect
client = TelegramClient(username, api_id, api_hash)

async def main(phone):
    await client.start()
    print("Client Created")
    # Ensure you're authorized
    if await client.is_user_authorized() == False:
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    user_input_channel = channel_url

    if user_input_channel.isdigit():
        entity = PeerChannel(int(user_input_channel))
    else:
        entity = user_input_channel

    my_channel = await client.get_entity(entity)

    # replace with your desired date, for all messages set data before telegram channel
    start_date = datetime(2023, 6, 4, 0, 0)  
    

    offset_id = 0
    limit = 100
    all_messages = []
    total_messages = 0
    total_count_limit = 0

    while True:
        # print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
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
            message_hm ={"date":message.date.isoformat(), "name": "HM", }
            message_max = {"date":message.date.isoformat(), "name": "Max"}
            # print(message)
            if message.date.date() >= start_date.date():
                if message.message is not None:
                    substring = re.search(r'LATAM(.*?)SMILES', message.message, re.DOTALL)
                    # print(substring)
                
                    if substring is not None:
                        # print("padroa")
                        substring = substring.group(0)
                        lines = substring.splitlines() 
                        for line in lines:
                            if "Max" in line:   
                                value = float(re.findall(r"\d+,\d+", line)[0].replace(",", "."))
                                message_max["Latam"] = value

                            if "Hot" in line:   
                                value = float(re.findall(r"\d+,\d+", line)[0].replace(",", "."))
                                message_hm["Latam"] = value                                 
                    
                    substring = re.search(r'SMILES(.*?)TUDO', message.message, re.DOTALL)
                    print(substring)
                
                    if substring is not None:
                        # print("padroa")
                        substring = substring.group(0)
                        lines = substring.splitlines() 
                        for line in lines:
                            if "Max" in line:   
                                value = float(re.findall(r"\d+,\d+", line)[0].replace(",", "."))
                                message_max["Gol"] = value

                            if "Hot" in line:   
                                value = float(re.findall(r"\d+,\d+", line)[0].replace(",", "."))
                                message_hm["Gol"] = value
                                          
                    substring = re.search(r'TUDO(.*?)TAP', message.message, re.DOTALL)
                    print(substring)
                
                    if substring is not None:
                        print("padroa")
                        substring = substring.group(0)
                        lines = substring.splitlines() 
                        for line in lines:
                            if "Max" in line:   
                                value = float(re.findall(r"\d+,\d+", line)[0].replace(",", "."))
                                message_max["Azul"] = value

                            if "Hot" in line:   
                                value = float(re.findall(r"\d+,\d+", line)[0].replace(",", "."))
                                message_hm["Azul"] = value              
        
            else:
                break  # We reached the end of the messages that match the criteria, so we can stop the loop
            
            
            #se for o caso, aqui, fazer um if para ver se tem alguma cia faltando e não add
            all_messages.append(message_hm)
            all_messages.append(message_max)
        
        offset_id = messages[-1].id
        total_messages = len(all_messages)
        
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break
    print("Total Messages:", total_messages)
    # print(all_messages)

    with open('channel-messages2.json', 'w') as outfile:
        json.dump(all_messages, outfile, cls=DateTimeEncoder)

with client:
    client.loop.run_until_complete(main(phone))
