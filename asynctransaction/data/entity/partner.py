from ipaddress import ip_address
from urllib.parse import urlparse
from typing import Dict
import logging

from asynctransaction.data.entity.base import EntityBase, DbColumn

log = logging.getLogger('asynctransaction.data.entity.partner')


class Partner(EntityBase):
    """Partner entity class for all partners in the transaction process."""

    def __init__(self, **kwargs):
        kwargs['name'] = 'PARTNERS'
        super().__init__(**kwargs)
        if 'netloc' in kwargs:
            self.ip_address, self.port = self.parse_ip_port(kwargs['netloc'].replace('localhost', '127.0.0.1'))
        else:
            try:
                self.ip_address = ip_address(kwargs.get('IP_ADDRESS', '127.0.0.1').replace('localhost', '127.0.0.1'))
            except ValueError:
                log.error(f"No valid ip address at {kwargs.get('IP_ADDRESS','')}")
                self.ip_address = ip_address('127.0.0.1')
            self.port = kwargs.get('PORT', '80')
        self.description = kwargs.get('DESCRIPTION', '')
        self.columns.extend(
            {DbColumn(name='IP_ADDRESS', index=1),
             DbColumn(name='PORT', index=2, data_type='integer'),
             DbColumn(name='DESCRIPTION', index=3)})

    def is_local(self) -> bool:
        return self.ip_address.is_loopback

    @staticmethod
    def parse_ip_port(netloc):
        try:
            ip = ip_address(netloc)
            s_port = None
        except ValueError:
            parsed = urlparse('//{}'.format(netloc))
            ip = ip_address(parsed.hostname)
            s_port = parsed.port
        return ip, s_port

    def to_dict(self) -> Dict:
        return {**{'IP_ADDRESS': str(self.ip_address), 'PORT': self.port, 'DESCRIPTION': self.description},
                **super().to_dict()}
