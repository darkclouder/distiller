class SchedulingInfo:
    def __init__(
        self,
        spirit_id,
        age_requirement,
        next_exec_date,
        priority=0,
        reoccurring=False,
        start_date=None,
        end_date=None,
        schedule_id=None
    ):
        self.spirit_id = spirit_id
        self.age_requirement = age_requirement
        self.next_exec_date = next_exec_date
        self.priority = priority
        self.reoccurring = reoccurring
        self.start_date = start_date
        self.end_date = end_date
        self.schedule_id = schedule_id

    def __gt__(self, other_info):
        return self.next_exec_date > other_info.next_exec_date
