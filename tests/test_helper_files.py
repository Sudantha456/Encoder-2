"""Helper worker repos are populated from an explicit file map in dist.py.

A module that worker.py (or anything it imports) needs but that is missing
from the map makes every worker job fail right after checkout.
"""
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def imports_of(path):
    tree = ast.parse(path.read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    return names


class HelperFileMapTests(unittest.TestCase):
    def test_worker_dependencies_are_all_listed(self):
        import dist
        source = (ROOT / "dist.py").read_text()
        tree = ast.parse(source)
        listed = None
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict) and any(
                isinstance(k, ast.Constant) and k.value == "worker.py" for k in node.keys
            ):
                listed = {k.value for k in node.keys if isinstance(k, ast.Constant)}
                break
        self.assertIsNotNone(listed, "worker file map not found in dist.py")
        self.assertIn(".github/workflows/encode-worker.yml", listed)

        needed, queue = {"worker.py"}, ["worker.py"]
        while queue:
            name = queue.pop()
            for mod in imports_of(ROOT / name):
                local = ROOT / f"{mod}.py"
                if local.exists() and local.name not in needed:
                    needed.add(local.name)
                    queue.append(local.name)
        self.assertLessEqual(needed, listed, f"missing from helper file map: {needed - listed}")


if __name__ == "__main__":
    unittest.main()
