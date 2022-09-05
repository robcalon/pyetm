import re


class UnprossesableEntityError(Exception):
    pass

def format_share_group_error(error):
    """apply more readable format to
    share group errors messages"""

    # find share group
    pattern = re.compile("\"[a-z_]*\"")
    group = pattern.findall(error)[0]

    # find group total
    pattern = re.compile("\d*[.]\d*")
    group_sum = pattern.findall(error)[0]

    # reformat message
    group = group.replace("\"", "\'")
    group = "Share_group %s sums to %s" %(group, group_sum)

    # find parameters in group
    pattern = re.compile("[a-z_]*=[0-9.]*")
    items = pattern.findall(error)

    # reformat message
    items = [item.replace("=", "': ") for item in items]
    items = "'" + ",\n    '".join(items)

    return """%s\n   {%s}""" %(group, items)

def format_error_messages(errors):
    """format and handle error message"""
        
    # newlist
    errs = []

    # iterate over messages
    for error in errors:
        
        # format share group errors
        if "group does not balance" in error:
            error = format_share_group_error(error)

        # append to list
        errs.append(error)

    # make final message
    base = "ETEngine returned the following error(s):"
    msg = """%s\n > {}""".format("\n > ".join(errs)) %base

    return msg