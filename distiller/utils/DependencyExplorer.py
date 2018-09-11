from distiller.utils.TaskLoader import TaskLoader


class DependencyExplorer:
    @classmethod
    def build_graph(cls, target_spirit_id, skip_pipes=True, **kwargs):
        return cls.__explore(TaskLoader.init(target_spirit_id), {}, skip_pipes=skip_pipes, **kwargs)[2]

    @classmethod
    def involved_spirits(cls, target_spirit_id, skip_pipes=True, **kwargs):
        return list(cls.__explore(TaskLoader.init(target_spirit_id), {}, skip_pipes=skip_pipes, **kwargs)[0].keys())

    @classmethod
    def __explore(cls, target_spirit, nodes, used_spirits=None, used_task_count=None, skip_pipes=True, **kwargs):
        if "prune_func" in kwargs:
            prune_func = kwargs["prune_func"]
        else:
            prune_func = None

        target_task_id = target_spirit.name()

        if used_spirits is None:
            used_spirits = set()

        if used_task_count is None:
            used_task_count = {}

        all_parents = set()
        all_roots = set()

        skip = skip_pipes and TaskLoader.spirit_is_pipe(target_spirit)

        curr = cls.__get_node(nodes, target_spirit)

        if target_spirit in used_spirits:
            raise CyclicDependencyException(target_spirit.spirit_id())

        up_used_spirits = set(used_spirits)
        up_used_spirits.add(target_spirit)

        up_used_task_count = dict(used_task_count)

        if target_task_id not in up_used_task_count:
            up_used_task_count[target_task_id] = target_spirit.occurrences() - 1
        else:
            up_used_task_count[target_task_id] -= 1

        if up_used_task_count[target_task_id] < 0:
            raise CyclicDependencyException(target_task_id, task=True)

        for dep in target_spirit.requires():
            dep_spirit = TaskLoader.init(dep)

            if prune_func is not None:
                if prune_func(dep_spirit):
                    print("Prune %s" % dep_spirit)
                    continue

            _, parents, roots = cls.__explore(
                dep_spirit,
                nodes,
                used_spirits=up_used_spirits,
                used_task_count=up_used_task_count,
                skip_pipes=skip_pipes
            )

            all_parents.update(parents)
            all_roots.update(roots)

        if skip:
            return nodes, all_parents, all_roots
        else:
            for parent in all_parents:
                curr.add_parent(parent)

            if len(all_roots) == 0:
                assert len(all_parents) == 0

                all_roots = [curr]

            return nodes, [curr], all_roots

    @classmethod
    def __get_node(cls, nodes, spirit):
        if spirit not in nodes:
            nodes[spirit] = DependencyNode(spirit)

        return nodes[spirit]


class DependencyNode:
    def __init__(self, spirit):
        self.spirit = spirit
        self.parents = []
        self.children = []

    def add_parent(self, parent):
        self.parents.append(parent)
        parent.children.append(self)

        return self

    def remove_parent(self, parent):
        self.parents.remove(parent)
        parent.children.remove(self)

    def __repr__(self):
        rep = repr(self.spirit)

        if len(self.children) > 0:
            rep += "<-(" + ";".join(
                [repr(child) for child in sorted(self.children, key=lambda x: x.spirit.label())]
            ) + ")"

        return rep

    def __eq__(self, other):
        return self.spirit == other.spirit

    def __hash__(self):
        return hash(self.spirit)


class CyclicDependencyException(Exception):
    def __init__(self, identifier, task=False):
        if task:
            super().__init__(
                "Cyclic dependency detected for %s. Check `AbstractTask.occurrences` to resolve this." % identifier
            )
        else:
            super().__init__("Cyclic dependency detected for %s." % identifier)
