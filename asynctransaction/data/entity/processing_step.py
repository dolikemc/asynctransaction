from typing import Dict
import json

from asynctransaction.data.entity.base import DbColumn, EntityBaseWithState


class ProcessingStep(EntityBaseWithState):
    """
    Entity class for a processing of a transaction
    """

    def __init__(self, **kwargs):
        kwargs['name'] = 'PROCESSING_STEPS'
        super().__init__(**kwargs)
        self.task_id = kwargs.get('TASK_ID', 0)
        self.partner_id = kwargs.get('PARTNER_ID', 0)
        self.local_id = kwargs.get('LOCAL_ID', 0)
        self.partner_id = kwargs.get('PARTNER_ID', 0)
        self.event_id = kwargs.get('EVENT_ID', 0)
        self.data = kwargs.get('DATA', json.dumps({}))
        self.ip_address = kwargs.get('IP_ADDRESS', 'localhost')
        self.port = kwargs.get('PORT', '80')
        self.description = kwargs.get('DESCRIPTION', '')
        self.columns.extend(
            {DbColumn(name='TASK_ID', index=1, fk='TASKS(ID)', data_type='integer'),
             DbColumn(name='PARTNER_ID', index=2, fk='PARTNERS(ID)', data_type='integer')})

        self._method = 'POST'
        self._url = 'orders'

    @property
    def method(self):
        #  todo: get from the event cache
        return self._method

    @property
    def url(self):
        #  todo: get from the event cache
        return self._url

    def to_dict(self) -> Dict:
        return {**{'TASK_ID': self.task_id,
                   'PARTNER_ID': self.partner_id,
                   'LOCAL_ID': self.local_id,
                   'EVENT_ID': self.event_id,
                   'DATA': self.data,
                   'DESCRIPTION': self.description,
                   'IP_ADDRESS': self.ip_address,
                   'PORT': self.port},
                **super().to_dict()}
