import sqlite3
import asyncio
import logging

from asynctransaction.data.entity import *

log = logging.getLogger('asynctransaction.data.access.base')


class MixedEntitiesException(ValueError):
    def __init__(self, *args):
        super().__init__(*args)


class EntityNotFound(IndexError):
    def __init__(self, *args):
        super().__init__(args)


class DataAccessBase(object):
    def __init__(self, con: sqlite3.Connection, name: str = '',
                 loop: asyncio.AbstractEventLoop = None):
        if loop is None:
            self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        else:
            self._loop: asyncio.AbstractEventLoop = loop
        self.data: List[EntityBase] = []
        self.connection: sqlite3.Connection = con
        self._name: str = name.upper()  # could also be set after data are populated

    @property
    def loop(self):
        return self._loop

    @property
    def name(self):
        if self._name == '':
            for index, record in enumerate(self.data):
                if index == 0:
                    self._name = record.name.upper()
                if self._name != record.name.upper():
                    raise MixedEntitiesException
        return self._name

    def get_result(self, uniqueness_expected: bool = True) -> EntityBase:
        if len(self.data) == 0:
            raise EntityNotFound(f"no entity found for {self.name}")
        if uniqueness_expected and len(self.data) > 1:
            raise EntityNotFound(f"less records than expected for entity {self.name}")
        return self.data[0]

    async def read(self, entity_id: int = 0, no_join: bool = False, by_state: bool = False) -> int:
        if self.name not in DataAccessBase._allowed_entities():
            return 0
        entity = self._entity_factory(row={'ID': entity_id})
        sql = entity.create_select_statement(read_all=(entity_id == 0), no_joins=no_join, by_state=by_state)
        future: asyncio.Future = self._loop.create_future()
        asyncio.ensure_future(self._execute_select(sql=sql, parameters=[entity_id], future=future))
        await future
        return future.result()

    async def update_state(self, to_state: int) -> int:
        if len(self.data) == 0:
            return 0
        parameters = {'ID': self.data[0].id, 'STATE': to_state}
        sql = self.data[0].create_update_statement(**parameters)
        future: asyncio.Future = self._loop.create_future()
        asyncio.ensure_future(self._execute_one(sql, [parameters], future))
        await future
        return future.result()

    async def insert(self) -> int:
        if len(self.data) == 0:
            return 0
        sql = self.data[0].create_insert_statement()
        future: asyncio.Future = self._loop.create_future()
        asyncio.ensure_future(self._execute_one(sql, [x.to_dict() for x in self.data], future))
        await future
        return future.result()

    async def _execute_select(self, sql: str, parameters: List, future: asyncio.Future):
        self.data.clear()
        try:
            with self.connection as con:
                for row in con.execute(sql, parameters):
                    self.data.append(self._entity_factory(row))
            future.set_result(len(self.data))
        except sqlite3.DatabaseError as error:
            future.set_exception(error)

    async def _execute_one(self, sql: str, parameters: List, future: asyncio.Future):
        try:
            with self.connection as con:
                if len(parameters) == 1 and sql.startswith('INSERT'):
                    cur = con.execute(sql, parameters[0])
                    future.set_result(cur.lastrowid)

                else:
                    cur = con.executemany(sql, parameters)
                    future.set_result(cur.rowcount)

        except sqlite3.DatabaseError as error:
            future.set_exception(error)

    def _entity_factory(self, row: Dict) -> EntityBase:
        if self.name not in DataAccessBase._allowed_entities():
            raise NoValidEntity
        # just make from CLASS_NAME a ClassName(**row) call
        return eval(''.join([x.capitalize() for x in self.name[:-1].split('_')]))(**row)

    @classmethod
    def _allowed_entities(cls):
        return {'PROCESSING_STEPS', 'TASKS', 'PARTNERS', 'EVENTS', 'SUBSCRIBERS'}


def row_to_dict(cursor: sqlite3.Cursor, row: sqlite3.Row) -> Dict:
    a = {}
    for index, col in enumerate(cursor.description):
        a[col[0].upper()] = row[index]
    return a


def prepare_connection(database_name: str = ':memory:') -> sqlite3.Connection:
    connection = sqlite3.connect(database_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    connection.row_factory = row_to_dict
    connection.execute('PRAGMA foreign_keys=ON;')
    return connection
