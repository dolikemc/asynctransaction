from typing import Dict
import json

from asynctransaction.data.entity.base import DbColumn, EntityBaseWithState


class TaskException(Exception):
    def __init__(self):
        super().__init__()


class Task(EntityBaseWithState):
    def __init__(self, **kwargs):
        kwargs['name'] = 'TASKS'
        super().__init__(**kwargs)
        self.local_id = kwargs.get('LOCAL_ID', 0)
        self.partner_id = kwargs.get('PARTNER_ID', 0)
        self.event_id = kwargs.get('EVENT_ID', 0)
        self.data = kwargs.get('DATA', json.dumps({}))
        if self.local_id == 0:
            try:
                data_dict = json.loads(self.data)
                if 'ID' in data_dict:
                    self.local_id = data_dict['ID']
            except json.JSONDecodeError:
                raise TaskException
                # data from join
        self.url = kwargs.get('URL', '')
        self.method = kwargs.get('METHOD', 'POST')
        self.description = kwargs.get('DESCRIPTION', '')
        self.ip_address = kwargs.get('IP_ADDRESS', '')
        self.port = kwargs.get('PORT', 0)
        self.columns.extend({DbColumn(name='LOCAL_ID', index=1, data_type='integer'),
                             DbColumn(name='PARTNER_ID', fk='PARTNERS(ID)', index=2, data_type='integer'),
                             DbColumn(name='EVENT_ID', fk='EVENTS(ID)', index=3, data_type='integer'),
                             DbColumn(name='DATA', index=4)})

    def to_dict(self) -> Dict:
        return {**{'LOCAL_ID': self.local_id,
                   'PARTNER_ID': self.partner_id,
                   'EVENT_ID': self.event_id,
                   'DATA': self.data,
                   'URL': self.url,
                   'METHOD': self.method,
                   'DESCRIPTION': self.description,
                   'IP_ADDRESS': self.ip_address,
                   'PORT': self.port},
                **super().to_dict()}
