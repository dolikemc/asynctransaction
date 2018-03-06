import nose.tools as nt
from ipaddress import IPv4Address
from datetime import datetime
import sqlite3
import asyncio
from multidict import CIMultiDict
import logging

from aiohttp.test_utils import unittest_run_loop

from asynctransaction.data.access.base import prepare_connection
from asynctransaction.data.entity.partner import Partner
from asynctransaction.data.access.partner import Partner as PartnerAccess

log = logging.getLogger('test.partner')


class TestPartner:
    def __init__(self):
        self.record = {}
        self.dbh: sqlite3.Connection = None
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()

    def setup(self):
        self.record = {'ID': 12, 'IP_ADDRESS': '122.1.2.3', 'PORT': 4040, 'DELETED': True}
        self.dbh = prepare_connection()
        cursor: sqlite3.Cursor = self.dbh.cursor()
        with open('asynctransaction/data/model/transaction.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))
        with open('asynctransaction/data/model/test_data.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))

    def teardown(self):
        self.dbh.close()

    def test_ctor1(self):
        test = Partner(netloc='145.123.45.2')
        nt.assert_equal(test.port, None)
        nt.assert_equal(test.ip_address, IPv4Address('145.123.45.2'))
        nt.assert_false(test.is_local())

    def test_ctor2(self):
        test = Partner(netloc='145.123.45.2:8080')
        nt.assert_equal(test.port, 8080)
        nt.assert_equal(test.ip_address, IPv4Address('145.123.45.2'))
        nt.assert_false(test.is_local())

    def test_ctor3(self):
        test = Partner(netloc='127.0.0.1:3010')
        nt.assert_equal(test.port, 3010)
        nt.assert_equal(test.ip_address, IPv4Address('127.0.0.1'))
        nt.assert_true(test.is_local())

    def test_bad_ctor(self):
        with nt.assert_raises(ValueError):
            Partner(netloc='')
        with nt.assert_raises(ValueError):
            Partner(netloc=':80')
        p = Partner(IP_ADDRESS='x2r')
        nt.assert_true(p.is_local())

    def test_record_ctor(self):
        test = Partner(**self.record)
        nt.assert_equal(test.port, 4040)
        nt.assert_less_equal(test.created_on, datetime.now())
        nt.assert_less_equal(test.updated_on, datetime.now())
        nt.assert_equal(test.deleted, True)
        nt.assert_equal(test.id, 12)
        nt.assert_equal(test.name, 'PARTNERS')

    def test_to_dict(self):
        test = Partner(**self.record)
        nt.assert_dict_contains_subset(self.record, test.to_dict())

    @unittest_run_loop
    async def test_get_partner_data(self):
        pa = PartnerAccess(con=self.dbh)
        partner = await pa.get_partner_data(IP_ADDRESS='127.0.0.1', PORT=3030)
        nt.assert_is_instance(partner, Partner)
        nt.assert_equal(partner.id, 1)

    @unittest_run_loop
    async def test_change_partner_data(self):
        pa = PartnerAccess(con=self.dbh)
        self.record['ID'] = 2
        partner = await pa.change_partner_data(**self.record)
        nt.assert_is_instance(partner, Partner)
        nt.assert_equal(str(partner.ip_address), self.record['IP_ADDRESS'])
        nt.assert_equal(partner.id, 2)
        partner.id = 0
        partner = await pa.change_partner_data(**partner.to_dict())
        nt.assert_is_instance(partner, Partner)
        nt.assert_equal(partner.id, 3)

    def test_ctor_multi_dict(self):
        multi_dict = CIMultiDict(self.record)
        partner = Partner(**multi_dict)
        nt.assert_is_instance(partner, Partner)
        nt.assert_equal(partner.id, 12)
