
def extend(target_dict, override):
    if override is not None:
        for (key, value) in override.items():
            if key in target_dict and isinstance(target_dict[key], dict):
                extend(target_dict[key], value)
            else:
                target_dict[key] = value
