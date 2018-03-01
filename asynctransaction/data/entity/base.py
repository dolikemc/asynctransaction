from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List

from asynctransaction.data.entity.state import State


class DbColumn(object):
    """
    DbColumn class to administrate the entity - data base relationship
    """

    def __init__(self, name: str, index: int, fk: str = None, data_type: str = 'TEXT', default=None):
        """
        Constructor of DbColumn
        :param name: Name of the column [str]
        :param index: Position of the column in the table [int >0]
        :param fk: Optional: If true, column is a foreign key. Default is False [bool]
        :param data_type: Optional: Type of column. Default is TEXT. Possible values are INTEGER, FLOAT, TEXT or
            TIMESTAMP [str]
        :param default: Optional: Default value of the column. If set to 'default' INTEGER and FLOAT take 0 as
            default, TIMESTAMP datetime('now','localtime') [str]
        """
        self.name = name
        self.data_type = data_type.upper()
        self.index = index
        self.fk = fk
        if default is None:
            self.default = ''
            return
        if isinstance(default, str) and default == 'default':
            if self.data_type in {'INTEGER', 'FLOAT'}:
                self.default = 'DEFAULT(0)'
            elif self.data_type == 'TIMESTAMP':
                self.default = "DEFAULT(datetime('now','localtime'))"
            else:
                self.default = "DEFAULT('')"
        else:
            self.default = f'DEFAULT({default})'


class NoValidEntity(Exception):
    """
    No valid entity exception. Occurs if no entity is implemented fro the given name.
    """

    def __init__(self, *args):
        super().__init__(*args)


class EntityBase(ABC):
    """
    All entities are derived from this base class. Each table has a couple of basic columns like created_on and
    deleted flag.
    """

    def __init__(self, name: str, **kwargs):
        """
        Constructor
        :param name: Name of the entity class related to the table name. Should be in upper letters and plural.
        :param kwargs: Expect a data base result row as dictionary. ID, UPDATED_ON, CREATED_ON, STATE and DELETED
            are linked to the class attributes id, updated_on, created_on, state and deleted. The columns
            attribute of type List[DbColumns] is initialized with the columns UPDATED_ON, CREATED_ON, STATE and DELETED
        """
        self.name = name
        self.id = int(kwargs.get('ID', 0))
        self.updated_on = kwargs.get('UPDATED_ON', datetime.now())
        self.created_on = kwargs.get('CREATED_ON', datetime.now())
        self.deleted = kwargs.get('DELETED', False)
        self.columns: List[DbColumn] = [
            DbColumn('UPDATED_ON', 101, None, 'TIMESTAMP', 'default'),
            DbColumn('CREATED_ON', 102, None, 'TIMESTAMP', 'default'),
            DbColumn('DELETED', 103, None, 'INTEGER', 'default')]

    @abstractmethod
    def to_dict(self) -> Dict:
        """
        Each entity class has to extend this method for the dictionary representation of itself
        :return: dictionary of all attributes and their values
        """
        return {'ID': self.id, 'CREATED_ON': self.created_on, 'UPDATED_ON': self.updated_on,
                'DELETED': self.deleted, 'name': self.name}

    def get_joins(self) -> List:
        return [x for x in sorted(filter(lambda x: x.fk is not None, self.columns), key=lambda x: x.index)]

    def create_table_statement(self) -> str:
        statement: List[str] = [f"CREATE TABLE {self.name} (\n    ID INTEGER PRIMARY KEY,\n"]

        for column in sorted(self.columns, key=lambda x: x.index):
            statement.append('    ')
            statement.append(f"{column.name} {column.data_type} {column.default}")
            statement.append(',\n')
        for column in self.get_joins():
            statement.append('    ')
            statement.append(
                f'CONSTRAINT FK_{self.name}_{column.name} foreign key({column.name}) REFERENCES {column.fk}')
            statement.append(',\n')
        statement.pop()
        statement.append(');')
        return ''.join(statement)

    def create_insert_statement(self) -> str:
        sql: List[str] = [f'INSERT INTO {self.name} (']
        for column in self.columns:
            if column.default == '':
                sql.append(column.name)
                sql.append(', ')
        sql.pop()
        sql.append(') VALUES (')
        for column in self.columns:
            if column.default == '':
                sql.append(':')
                sql.append(column.name)
                sql.append(', ')
        sql.pop()
        sql.append(');')
        return ''.join(sql)

    def create_select_statement(self, read_all: bool = True, by_state: bool = False, no_joins: bool = False) -> str:
        sql: List[str] = [f"SELECT *, '{self.name}' name FROM "]
        if no_joins:
            join_list = []
        else:
            join_list: List = self.get_joins()
        for joins in join_list:
            table, __ = joins.fk.split('(')
            sql.append(f'{table}, ')
        if read_all:
            sql.append(f'{self.name}\n    WHERE {self.name}.DELETED = ? ')
        elif by_state:
            sql.append(f'{self.name}\n    WHERE {self.name}.STATE = ? ')
        else:
            sql.append(f'{self.name}\n    WHERE {self.name}.ID = ? ')
        for joins in join_list:
            sql.append('AND ')
            table, key = joins.fk.split('(')
            key = key.replace(')', '')
            sql.append(f'{table}.{key} = {self.name}.{joins.name} ')
        sql.append(';')
        return ''.join(sql)

    def create_update_statement(self, **kwargs) -> str:
        sql: List[str] = [f'UPDATE {self.name} SET ']
        for column in self.columns:
            if column.name in kwargs.keys() and column.name not in {'ID', 'UPDATED_ON', 'CREATED_ON'}:
                sql.append(column.name)
                sql.append('= :')
                sql.append(column.name)
                sql.append(', ')
        sql.append("UPDATED_ON = datetime('now','localtime') WHERE ID = :ID;")
        return ''.join(sql)


class EntityBaseWithState(EntityBase):
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, **kwargs)
        self.state = kwargs.get('STATE', State.New.code)
        self.columns.append(DbColumn(name='STATE', index=99, fk=None, data_type='INTEGER', default=1))

    def to_dict(self):
        return {**{'STATE': self.state}, **super().to_dict()}

    def create_update_statement(self, **kwargs):
        return super().create_update_statement(**kwargs)
