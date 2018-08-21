import unittest

from distiller.utils.TaskLoader import TaskLoader
from distiller.utils.DependencyExplorer import DependencyExplorer, DependencyNode


class TestDependencyExplorer(unittest.TestCase):
    def setUp(self):
        self.t1 = TaskLoader.init(("testing.parameter_requires_pipe", {"requires":[], "id": 1}))
        self.t2 = TaskLoader.init(("testing.parameter_requires", {"requires":[self.t1.spirit_id()], "id": 2}))
        self.t3 = TaskLoader.init(("testing.parameter_requires", {"requires": [self.t2.spirit_id()], "id": 3}))
        self.t4 = TaskLoader.init(("testing.parameter_requires_pipe", {"requires": [self.t3.spirit_id()], "id": 4}))
        self.t5 = TaskLoader.init(("testing.parameter_requires", {"requires": [self.t4.spirit_id()], "id": 5}))
        self.t6 = TaskLoader.init(("testing.parameter_requires", {"requires": [], "id": 6}))
        self.t7 = TaskLoader.init(("testing.parameter_requires_pipe", {"requires": [self.t6.spirit_id()], "id": 7}))
        self.t8 = TaskLoader.init(("testing.parameter_requires", {"requires": [], "id": 8}))
        self.t9 = TaskLoader.init((
            "testing.parameter_requires",
            {"requires": [self.t7.spirit_id(), self.t8.spirit_id()], "id": 9}
        ))
        self.t10 = TaskLoader.init(("testing.parameter_requires", {"requires": [self.t7.spirit_id()], "id": 10}))
        self.t11 = TaskLoader.init((
            "testing.parameter_requires",
            {"requires": [self.t5.spirit_id(), self.t9.spirit_id(), self.t10.spirit_id()], "id": 11}
        ))

    def test_single_branch(self):
        self.assertEqual(self._get_roots(self.t1), set())
        self.assertEqual(self._get_roots(self.t2), {self.t2})
        self.assertEqual(self._get_roots(self.t3), {self.t2})
        self.assertEqual(self._get_roots(self.t4), {self.t2})
        self.assertEqual(self._get_roots(self.t5), {self.t2})

    def test_full(self):
        self.assertEqual(self._get_roots(self.t11), {self.t2, self.t6, self.t8})

        n2 = DependencyNode(self.t2)
        n3 = DependencyNode(self.t3)
        n5 = DependencyNode(self.t5)
        n6 = DependencyNode(self.t6)
        n8 = DependencyNode(self.t8)
        n9 = DependencyNode(self.t9)
        n10 = DependencyNode(self.t10)
        n11 = DependencyNode(self.t11)

        n11.add_parent(n5).add_parent(n9).add_parent(n10)
        n5.add_parent(n3)
        n3.add_parent(n2)
        n9.add_parent(n6).add_parent(n8)
        n10.add_parent(n6)

        full_graph = {_repr(root) for root in DependencyExplorer.explore(self.t11.spirit_id())}
        self.assertEqual(full_graph, {_repr(n2), _repr(n6), _repr(n8)})

    def _get_roots(self, spirit):
        return {root.spirit for root in DependencyExplorer.explore(spirit.spirit_id())}


def _repr(curr):
    rep = "%i" % curr.spirit.parameters["id"]

    if len(curr.children) > 0:

        rep += "<-(" + ";".join(
            [_repr(child) for child in sorted(curr.children, key=lambda x: x.spirit.parameters["id"])]
        ) + ")"

    return rep

if __name__ == "__main__":
    unittest.main()
