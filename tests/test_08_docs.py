"""Offline checks on SKILL.md as the agent actually receives it."""
from __future__ import annotations

import os
import re
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestSkillSurvivesArgumentSubstitution(unittest.TestCase):
    """Claude Code substitutes `$ARGUMENTS`, `$ARGUMENTS[N]` and `$N` in a
    skill body with the words the user typed after the slash command.
    Measured 2026-10-23: `/twitterapi-io use your biggest brain ...` rendered
    "100,000 credits = $1" as "= your" and "$2,000" as "biggest,000", so the
    cost table was unreadable exactly when the skill was invoked with a
    request. Amounts are written as "USD n" instead."""

    def test_no_argument_placeholders_in_skill_body(self):
        with open(os.path.join(ROOT, "SKILL.md"), encoding="utf-8") as f:
            text = f.read()
        hits = [(n, line) for n, line in enumerate(text.splitlines(), 1)
                if re.search(r"\$(\d|ARGUMENTS)", line)]
        self.assertEqual(hits, [], "write amounts as `USD n`; `$<digit>` is "
                         "replaced by the user's slash-command arguments")


if __name__ == "__main__":
    unittest.main()
