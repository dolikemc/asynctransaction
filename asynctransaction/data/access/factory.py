from asynctransaction.data.access.transaction_if import *
from asynctransaction.data.access.transaction import Transaction
from asynctransaction.data.access.event import Event
from asynctransaction.data.access.partner import Partner
from asynctransaction.data.access.task import Task
from asynctransaction.data.access.processing_step import ProcessingStep
from asynctransaction.data.access.subscriber import Subscriber


def create_transaction(con, transaction_type: str = 'default') -> ITransaction:
    if transaction_type == 'default':
        return Transaction(con)
    raise NotImplementedError


def create_event_access(con, event_type: str = 'default') -> IEventAccess:
    if event_type == 'default':
        return Event(con)
    raise NotImplementedError


def create_partner_access(con, partner_type: str = 'default') -> IPartnerAccess:
    if partner_type == 'default':
        return Partner(con)
    raise NotImplementedError


def create_task_access(con, task_type: str = 'default') -> ITaskAccess:
    if task_type == 'default':
        return Task(con)
    raise NotImplementedError


def create_processing_step_access(con, task_type: str = 'default') -> IProcessingStepsAccess:
    if task_type == 'default':
        return ProcessingStep(con)
    raise NotImplementedError


def create_subscriber_access(con, task_type: str = 'default') -> ISubscriberAccess:
    if task_type == 'default':
        return Subscriber(con)
    raise NotImplementedError
