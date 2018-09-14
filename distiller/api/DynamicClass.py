# FIXME: remove this hacky workaround to do class checks across multiple module loads


def class_id(_class_id):
    def decorator(cls):
        class_ids = [_class_id] + [cls_id for base in cls.__bases__ for cls_id in base._class_ids]
        cls._class_ids = class_ids

        return cls
    return decorator


class DynamicClass:
    _class_ids = ["DynamicClass"]

    @classmethod
    def class_id(cls):
        return cls._class_ids[0]

    @classmethod
    def inherits(cls, other):
        for cls_id in cls._class_ids:
            if cls_id == other:
                return True

        return False

    @classmethod
    def is_instance(cls, instance):
        return cls.class_id() == instance.class_id()
