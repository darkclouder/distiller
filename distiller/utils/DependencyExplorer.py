import collections

from distiller.api.DefaultPipe import DefaultPipe
from distiller.utils.TaskLoader import TaskLoader


class DependencyExplorer:
    @classmethod
    def explore(cls, target_spirit_id, skip_pipes=True):
        return cls.__explore(TaskLoader.init(target_spirit_id), {}, skip_pipes=skip_pipes)[1]

    @classmethod
    def __explore(cls, target_spirit, nodes, skip_pipes=True):
        all_parents = set()
        all_roots = set()

        skip = skip_pipes and issubclass(target_spirit.__class__, DefaultPipe)

        curr = cls.__get_node(nodes, target_spirit)

        for dep in target_spirit.requires():
            dep_spirit = TaskLoader.init(dep)

            parents, roots = cls.__explore(dep_spirit, nodes, skip_pipes=skip_pipes)

            all_parents.update(parents)
            all_roots.update(roots)

        if skip:
            return all_parents, all_roots
        else:
            for parent in all_parents:
                curr.add_parent(parent)

            if len(all_roots) == 0:
                assert len(all_parents) == 0

                all_roots = [curr]

            return [curr], all_roots

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