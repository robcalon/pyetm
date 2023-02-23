import unittest
import pandas as pd

from fixtures import reg, mapping, curves
from pyetm.utils import categorise_curves, regionalise_curves, regionalise_node

class UtilsTester(unittest.TestCase):

    def test_categorise_curves(self):

        mapped = categorise_curves(curves, mapping)
        sol = pd.DataFrame({
            'deficit': [0, 0, 0, 0, 166],
            'mapping_a': [5, 30, -66, 32,-129],
            'mapping_b': [-5, -30, 66, -32, -37],
        })

        self.assertEqual(True, mapped.equals(sol))

    def test_regionalise_curves(self):

        rec = regionalise_curves(curves, reg)
        sol = pd.DataFrame({
            'node_a': [135.0, 102.0, 21.0, 117.0, 34.0],
            'node_b': [88.0, 102.0, 45.0, 127.0, 95.0],
            'node_c': [33.0,  5.0, 90.0, 37.0,  1.0],
            'node_d': [38.0, 35.0, 24.0, 65.0, 36.0],
            'node_e': [0.0, 0.0, 0.0, 0.0, 166.0],
        })

        self.assertEqual(True, rec.equals(sol.T))

    def test_regionalise_node(self):

        rec = regionalise_node(curves, reg, "node_a")
        sol = pd.DataFrame({
            'category_a.input (MW)': [40.0, 35.0, 21.0, 42.0, 34.0],
            'category_b.input (MW)': [0.0, 0.0, 0.0, 0.0, 0.0],
            'category_c.input (MW)': [0.0, 0.0, 0.0, 0.0, 0.0],
            'category_d.input (MW)': [0.0, 0.0, 0.0, 0.0, 0.0],
            'category_a.output (MW)': [95.0, 67.0, 0.0, 75.0, 0.0],
            'category_b.output (MW)': [0.0, 0.0, 0.0, 0.0, 0.0],
            'category_c.output (MW)': [0.0, 0.0, 0.0, 0.0, 0.0],
            'deficit': [0.0, 0.0, 0.0, 0.0, 0.0]
        })

        self.assertEqual(True, rec.equals(sol))

if __name__ == "__main__":
    unittest.main()