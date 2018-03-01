from typing import Dict

from asynctransaction.data.entity.base import DbColumn, EntityBase


class Event(EntityBase):
    def __init__(self, **kwargs):
        kwargs['name'] = 'EVENTS'
        super().__init__(**kwargs)
        self.url = kwargs.get('URL', '')
        self.method = kwargs.get('METHOD', 'POST')
        self.description = kwargs.get('DESCRIPTION', '')
        self.columns.extend(
            {DbColumn(name='URL', index=1),
             DbColumn(name='METHOD', index=2, default='POST'),
             DbColumn(name='DESCRIPTION', index=3, default='default')})

    def to_dict(self) -> Dict:
        return {**{'METHOD': self.method, 'URL': self.url, 'DESCRIPTION': self.description},
                **super().to_dict()}
