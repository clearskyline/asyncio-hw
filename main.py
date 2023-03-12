import datetime

import asyncpg
import asyncio
import aiohttp
from more_itertools import chunked
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import JSON, Integer, Column

from config_file import POSTGRES_DSN


engine = create_async_engine(POSTGRES_DSN)


Base = declarative_base()
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


class SwapiCharacters(Base):

    __tablename__ = 'swapi_characters'
    id = Column(Integer, primary_key=True)
    json_info = Column(JSON)


NUMBER_OF_ITEMS = 100
CHUNK_SIZE = 10


async def chunked_async(async_iter, size):

    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            if buffer:
                yield buffer
            break
        if item:
            buffer.append(item)
            if len(buffer) == size:
                yield buffer
                buffer = []


async def more_info(link_list, client_session):
    cors = [client_session.get(link) for link in link_list]
    responses = await asyncio.gather(*cors)
    json_response = [response.json() for response in responses]
    return await asyncio.gather(*json_response)


async def db_insert(character_list):
    async with Session() as session:
        all_characters = [SwapiCharacters(json_info=character) for character in character_list]
        session.add_all(all_characters)
        await session.commit()


async def get_character(character_id, client_session):
    async with client_session.get(f'https://swapi.dev/api/people/{character_id}') as response:
        json_data = await response.json()
        if json_data.get('detail') == 'Not found':
            return None
        else:
            all_films = more_info(json_data.get('films', []), client_session)
            all_species = more_info(json_data.get('species', []), client_session)
            all_starships = more_info(json_data.get('starships', []), client_session)
            all_vehicles = more_info(json_data.get('vehicles', []), client_session)
            fields = await asyncio.gather(all_films, all_species, all_starships, all_vehicles)
            films, species, starships, vehicles = fields
            final_json = {
                'id': character_id,
                'birth_year': json_data.get('birth_year'),
                'eye_color': json_data.get('eye_color'),
                'films': ", ".join(film['title'] for film in films),
                'gender': json_data.get('gender'),
                'hair_color': json_data.get('hair_color'),
                'height': json_data.get('height'),
                'homeworld': json_data.get('homeworld'),
                'mass': json_data.get('mass'),
                'name': json_data.get('name'),
                'skin_color': json_data.get('skin_color'),
                'species': ", ".join(spec['name'] for spec in species),
                'starships': ", ".join(starship['name'] for starship in starships),
                'vehicles': ", ".join(vehicle['name'] for vehicle in vehicles)
            }
            return final_json


async def get_all_characters():
    async with aiohttp.ClientSession() as client_session:
        for chunk in chunked(range(1, NUMBER_OF_ITEMS), CHUNK_SIZE):
            cors = [get_character(i, client_session) for i in chunk]
            results = await asyncio.gather(*cors)
            for item in results:
                yield item


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_all_characters(), CHUNK_SIZE):
        asyncio.create_task(db_insert(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task


start = datetime.datetime.now()
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
print(datetime.datetime.now() - start)
