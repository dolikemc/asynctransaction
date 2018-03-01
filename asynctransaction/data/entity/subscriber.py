from typing import Dict

from asynctransaction.data.entity.base import DbColumn, EntityBase


class Subscriber(EntityBase):
    def __init__(self, **kwargs):
        kwargs['name'] = 'SUBSCRIBERS'
        super().__init__(**kwargs)
        self.event_id = kwargs.get('EVENT_ID', 0)
        self.partner_id = kwargs.get('PARTNER_ID', 0)
        self.url = kwargs.get('URL', '')
        self.method = kwargs.get('METHOD', 'POST')
        self.description = kwargs.get('DESCRIPTION', '')
        self.ip_address = kwargs.get('IP_ADDRESS', '')
        self.port = kwargs.get('PORT', 0)
        self.columns.extend(
            {DbColumn(name='EVENT_ID', index=1, data_type='INTEGER', fk='EVENTS(ID)'),
             DbColumn(name='PARTNER_ID', index=2, data_type='INTEGER', fk='PARTNERS(ID)')})

    def to_dict(self) -> Dict:
        return {**{'EVENT_ID': self.event_id, 'PARTNER_ID': self.partner_id,
                   'METHOD': self.method, 'URL': self.url, 'DESCRIPTION': self.description,
                   'IP_ADDRESS': str(self.ip_address), 'PORT': self.port},
                **super().to_dict()}
