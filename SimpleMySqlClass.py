import asyncio

import aiomysql

class SimpleMySqlClass:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SimpleMySqlClass, cls).__new__(cls)
        return cls._instance

    def __init__(self, host, db_name, user, password):
        if not hasattr(self, 'pool'):
            self.host = host
            self.db_name = db_name
            self.user = user
            self.password = password
            self.pool = None

    @classmethod
    async def get_instance(cls, host, db_name, user, password):
        if not cls._instance or not cls._instance.pool:
            cls._instance = cls(host, db_name, user, password)
            await cls._instance.connect()
        return cls._instance

    async def connect(self):
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=3306,
            user=self.user,
            password=self.password,
            db=self.db_name,
            loop=asyncio.get_event_loop()
        )

    async def query_sql(self, sql):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                data = await cur.fetchall()
                return data

    async def execute(self, sql):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(sql)
                    await conn.commit()
                    return cur.lastrowid
                except Exception as e:
                    print(f'执行的SQL语句：{sql}, 出现异常，请检查: {str(e)}')
                    await conn.rollback()
                    return -1

    async def list_tables(self):
        sql = "SHOW TABLES"
        return await self.query_sql(sql)

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()