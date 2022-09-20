import asyncio
import asyncpg


async def make_request(db_pool, table_name):
    QUERY = f"""INSERT INTO diploma_db.dbo.{table_name} (name_1, name_2) VALUES ($1, $2)"""
    await db_pool.fetch(QUERY, 1, 'some string')


async def main():
    chunk = 5000
    tasks = []
    pended = 0
    db_pool = await asyncpg.create_pool('postgres://datagrip:datagrip@localhost:5432/diploma_db')

    for _ in range(16000):
        tasks.append(asyncio.create_task(make_request(db_pool, 'test_table')))
        pended += 1
        if len(tasks) == chunk or pended == 16000:
            await asyncio.gather(*tasks)
            tasks = []
            print(pended)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
