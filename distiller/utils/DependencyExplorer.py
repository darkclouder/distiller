from distiller.utils.TaskLoader import TaskLoader


class DependencyExplorer:
    @classmethod
    def build_graph(cls, target_spirit_id, **kwargs):
        return cls.__explore(
            TaskLoader.init(target_spirit_id),
            **kwargs
        )[2]

    @classmethod
    def involved_spirits(cls, target_spirit_id, **kwargs):
        return list(cls.__explore(
            TaskLoader.init(target_spirit_id),
            **kwargs
        )[0].keys())

    @classmethod
    def __explore(
            cls,
            current_spirit,
            all_nodes=None,
            downward_used_spirits=None,
            downward_occurrences=None,
            **kwargs
    ):
        enforce_func = kwargs.get("enforce_func", None)
        skip_pipes = kwargs.get("skip_pipes", True)

        if all_nodes is None:
            all_nodes = dict()
        if downward_used_spirits is None:
            downward_used_spirits = set()
        if downward_occurrences is None:
            downward_occurrences = dict()

        # Check if current spirit has already been used
        if current_spirit in downward_used_spirits:
            raise CyclicDependencyException(current_spirit.spirit_id())

        up_downward_used_spirits = set(downward_used_spirits)
        up_downward_used_spirits.add(current_spirit)

        # Check if the task exceeds its occurrence count
        up_downward_occurrences = dict(downward_occurrences)

        curr_task_id = current_spirit.name()

        if curr_task_id not in up_downward_occurrences:
            up_downward_occurrences[curr_task_id] = current_spirit.occurrences()

        up_downward_occurrences[curr_task_id] -= 1

        if up_downward_occurrences[curr_task_id] < 0:
            raise CyclicDependencyException(curr_task_id, task=True)

        parents = set()
        roots = set()

        for dep in current_spirit.requires():
            dep_spirit = TaskLoader.init(dep)

            _, dep_nodes, dep_roots = cls.__explore(
                dep_spirit,
                all_nodes=all_nodes,
                downward_used_spirits=up_downward_used_spirits,
                downward_occurrences=up_downward_occurrences,
                **kwargs
            )

            if dep_nodes is not None:
                parents.update(dep_nodes)
                roots.update(dep_roots)

        has_parents = len(parents) > 0
        # Default is all enforced
        build_enforced = enforce_func is None or enforce_func(current_spirit)
        is_pipe = TaskLoader.spirit_is_pipe(current_spirit)

        # No prune possible if upward needs to be built or build for this spirit is enforced.
        if skip_pipes and is_pipe:
            if has_parents:
                return all_nodes, parents, roots
            else:
                return all_nodes, None, []
        elif has_parents or build_enforced:
            curr_node = cls.__get_node(all_nodes, current_spirit)

            for parent in parents:
                curr_node.add_parent(parent)

            # No parents? This is the first with enforced build.
            # Therefore, it is the first root of this path.
            if not has_parents:
                roots = [curr_node]

            return all_nodes, [curr_node], roots
        else:
            return all_nodes, None, []

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
