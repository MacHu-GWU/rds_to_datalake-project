# -*- coding: utf-8 -*-

def is_role_exists(
    iam_client,
    role_name: str,
) -> bool:
    try:
        iam_client.get_role(RoleName=role_name)
        return True
    except Exception as e:
        if "not found" in str(e).lower():
            return False
        else:
            raise NotImplementedError
