#!/usr/bin/env python3
import unittest
from lmn.helpers import parse_config, merge_nested_dict

class TestMergeDicts(unittest.TestCase):
    def test_flat_dict(self):
        """Merging flat dictionaries"""
        a = {'apple': 1, 'orange': 2}
        b = {'pearl': 3, 'lemon': 4}
        merged = merge_nested_dict(a, b)
        self.assertDictEqual({**a, **b}, merged)

    def test_flat_dict_dup_a(self):
        a = {'apple': 1, 'orange': 2}
        b = {'apple': 3, 'lemon': 4}
        merged = merge_nested_dict(a, b, conflict='use_a')
        self.assertDictEqual({**a, 'lemon': 4}, merged)

    def test_flat_dict_dup_b(self):
        a = {'apple': 1, 'orange': 2}
        b = {'apple': 3, 'lemon': 4}
        merged = merge_nested_dict(a, b, conflict='use_b')
        self.assertDictEqual({**b, 'orange': 2}, merged)

    def test_nested_dict_dup(self):
        a = {
            'machines': {'slurm': 'hoge', 'birch': 'fuga'},
            'slurm-configs': {'two-cpus': 'yeah'},
            'docker-images': {'image0': 'foo'}
        }
        b = {
            'machines': {'elm': 'piyo'},
            'docker-images': {'image2': 'xxx'}
        }
        gold = {
            'machines': {'slurm': 'hoge', 'birch': 'fuga', 'elm': 'piyo'},
            'slurm-configs': {'two-cpus': 'yeah'},
            'docker-images': {'image0': 'foo', 'image2': 'xxx'}
        }
        merged = merge_nested_dict(a, b)
        self.assertDictEqual(gold, merged)

    def test_nested_dict_dup2(self):
        """Lists are not merged but simply overwritten!"""
        a = {
            'machines': {'slurm': ['hoge', 'fuga']},
            'slurm-configs': {'two-cpus': 'yeah'},
            'docker-images': {'image0': 'foo'}
        }
        b = {
            'machines': {'slurm': ['piyo']},
            'docker-images': {'image0': ['bar', 'piyo']}
        }
        gold = {
            'machines': {'slurm': ['piyo']},
            'slurm-configs': {'two-cpus': 'yeah'},
            'docker-images': {'image0': ['bar', 'piyo']}
        }
        merged = merge_nested_dict(a, b)
        self.assertDictEqual(gold, merged)


if __name__ == '__main__':
    unittest.main()
