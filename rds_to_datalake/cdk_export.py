# -*- coding: utf-8 -*-

import dataclasses
from .config_init import config
from .boto_ses import bsm


@dataclasses.dataclass
class Outputs:
    db_instance_host: str

    @classmethod
    def read(cls):
        res = bsm.cloudformation_client.describe_stacks(
            StackName=config.cloudformation_stack_name,
        )
        outputs = dict()
        for dct in res["Stacks"][0]["Outputs"]:
            outputs[dct["OutputKey"]] = dct["OutputValue"]
        return cls(
            db_instance_host=outputs[config.db_instance_host_output_id],
        )
