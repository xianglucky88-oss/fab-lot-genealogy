"""Mutation testing: prove the oracle battery kills semantic mutations
(mutation testing).

Each mutant is produced by a targeted source transform of a production
module, loaded in isolation, and checked with the shared oracle battery.
Every mutant must be killed; the unmutated control must pass.
"""
import types
import unittest
from pathlib import Path

import oracle_battery

SRC = Path(__file__).parents[1] / "src" / "fab_lot_genealogy"


def _replace(old, new):
    def transform(source):
        assert old in source, "mutation target missing: %r" % old
        return source.replace(old, new, 1)

    return transform


MUTANTS = [
    # --- domain computation layer (engine) ---
    ("engine:duplicates-counted", "engine",
     _replace("    unique = _dedup(scoped)",
              "    unique = list(scoped)  # mutation: keep duplicates")),
    ("engine:latest-by-input-order", "engine",
     _replace('    latest = max(unique, key=order)["to_batch"] if unique else None',
              '    latest = unique[-1]["to_batch"] if unique else None'
              "  # mutation: input order")),
    ("engine:earliest-as-latest", "engine",
     _replace('    latest = max(unique, key=order)["to_batch"] if unique else None',
              '    latest = min(unique, key=order)["to_batch"] if unique else None'
              "  # mutation: earliest")),
    ("engine:self-as-ancestor", "engine",
     _replace("        seen.discard(batch)",
              "        pass  # mutation: self can be its own ancestor")),
    ("engine:reversed-edges", "engine",
     _replace('        parents.setdefault(row["to_batch"], set()).add(row["from_batch"])',
              '        parents.setdefault(row["from_batch"], set()).add(row["to_batch"])'
              "  # mutation: reversed")),
    ("engine:unsorted-ancestors", "engine",
     _replace("        return sorted(seen)",
              "        return sorted(seen, reverse=True)  # mutation: order")),
    ("engine:wafer-filter-ignored", "engine",
     _replace('    scoped = [row for row in rows if wafer is None or row["wafer"] == wafer]',
              "    scoped = list(rows)  # mutation: ignore wafer filter")),
    ("engine:sources-missing-from-layers", "engine",
     _replace('"layers": {batch: ancestors(batch) for batch in sorted(batches)},',
              '"layers": {batch: ancestors(batch) for batch in sorted(parents)},'
              "  # mutation: sources missing")),
    ("engine:raw-operation-count", "engine",
     _replace('"operation_count": len(unique),',
              '"operation_count": len(rows),  # mutation: raw count')),
    # --- input parsing layer (parser) ---
    ("parser:illegal-kind-accepted", "parser",
     _replace('    if row["kind"] not in LEGAL_KINDS:',
              '    if row["kind"] not in LEGAL_KINDS and False:  # mutation')),
    ("parser:silent-drop", "parser",
     _replace('            rejected.append({"index": index, "reason": reason, "row": row})',
              "            pass  # mutation: silent drop")),
    ("parser:types-unchecked", "parser",
     _replace("        if not isinstance(row[field], str):",
              "        if not isinstance(row[field], str) and False:  # mutation")),
    # --- result encapsulation layer (report) ---
    ("report:data-overrides-envelope", "report",
     _replace('    return {**result.data, "project": result.project, "status": "ok"}',
              '    return {"project": result.project, "status": "ok", **result.data}'
              "  # mutation: data wins")),
    ("report:data-keys-dropped", "report",
     _replace('    return {**result.data, "project": result.project, "status": "ok"}',
              '    return {"project": result.project, "status": "ok"}'
              "  # mutation: drop data")),
    # --- flow orchestration layer (pipeline) ---
    ("pipeline:payload-context-dropped", "pipeline",
     _replace("    data = calculate(records, payload)",
              "    data = calculate(records, {})  # mutation: drop context")),
    ("pipeline:rejections-hidden", "pipeline",
     _replace('    data["rejected_operations"] = rejected',
              '    data["rejected_operations"] = []  # mutation: hide rejections')),
]


def load_mutated(module_name, transform):
    source = (SRC / ("%s.py" % module_name)).read_text()
    mutated = transform(source)
    assert mutated != source, "transform did not change %s" % module_name
    module = types.ModuleType("fab_lot_genealogy.%s_mutant" % module_name)
    module.__dict__["__package__"] = "fab_lot_genealogy"
    exec(compile(mutated, "<mutant:%s>" % module_name, "exec"), module.__dict__)
    return module


def impl_for(module_name, module):
    real = oracle_battery.real_impl()
    if module_name == "engine":
        return oracle_battery.Impl(
            real.parse_operations, module.calculate, real.build_result,
            oracle_battery.compose(real.parse_operations, module.calculate,
                                   real.build_result))
    if module_name == "parser":
        return oracle_battery.Impl(
            module.parse_operations, real.calculate, real.build_result,
            oracle_battery.compose(module.parse_operations, real.calculate,
                                   real.build_result))
    if module_name == "report":
        return oracle_battery.Impl(
            real.parse_operations, real.calculate, module.build_result,
            oracle_battery.compose(real.parse_operations, real.calculate,
                                   module.build_result))
    if module_name == "pipeline":
        return oracle_battery.Impl(real.parse_operations, real.calculate,
                                   real.build_result, module.run)
    raise AssertionError(module_name)


class TestMutation(unittest.TestCase):
    def test_unmutated_control_passes_battery(self):
        failures = oracle_battery.run_battery(
            oracle_battery.real_impl(), random_cases=60, include_slow=False)
        self.assertEqual([], [f.check for f in failures],
                         msg="\n\n".join(f.report for f in failures[:3]))

    def test_every_mutant_is_killed(self):
        survivors, killed_by = [], {}
        for name, module_name, transform in MUTANTS:
            module = load_mutated(module_name, transform)
            impl = impl_for(module_name, module)
            failures = oracle_battery.run_battery(
                impl, random_cases=60, include_slow=False)
            if failures:
                killed_by[name] = [f.check for f in failures]
            else:
                survivors.append(name)
        self.assertEqual([], survivors,
                         "mutants survived: %s" % survivors)
        # Every mutant must be killed by at least one *targeted* check, not
        # only by random exploration.
        for name, checks in killed_by.items():
            self.assertTrue(
                any(not c.startswith("random[") for c in checks),
                "mutant %s killed only by random cases: %s" % (name, checks))


if __name__ == "__main__":
    unittest.main()
