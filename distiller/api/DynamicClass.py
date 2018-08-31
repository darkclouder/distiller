# FIXME: remove this hacky workaround to do class checks across multiple module loads


class DynamicClass:
    @staticmethod
    def class_id():
        return "DynamicClass"
