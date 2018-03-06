import nose.tools as nt

from asynctransaction.data.entity.processing_step import ProcessingStep


class TestProcessing:
    def __init__(self):
        self.record = {}

    def setup(self):
        self.record = {'ID': 291, 'TASK_ID': 1, 'PARTNER_ID': 2}

    def test_ctor(self):
        test = ProcessingStep(task_id=0, partner_id=0)
        nt.assert_equals(test.task_id, 0)

    def test_ctor_record(self):
        step = ProcessingStep(**self.record)
        nt.assert_equal(step.id, 291)
        nt.assert_equal(step.task_id, 1)
        nt.assert_equal(step.partner_id, 2)

    def test_to_dict(self):
        step = ProcessingStep(**self.record)
        nt.assert_dict_contains_subset(self.record, step.to_dict())
