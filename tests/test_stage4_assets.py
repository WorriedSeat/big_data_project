"""Local checks for Stage IV artifacts (no cluster required)."""
from __future__ import annotations

import csv
import os
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _working_bash() -> str | None:
    candidates = []
    for p in (
        os.environ.get("GIT_BASH"),
        shutil.which("bash"),
        r"C:\Program Files\Git\bin\bash.exe",
    ):
        if p and Path(p).exists() and p not in candidates:
            candidates.append(p)
    for exe in candidates:
        try:
            subprocess.run(
                [exe, "--version"],
                check=True,
                capture_output=True,
                timeout=15,
            )
            return exe
        except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
            continue
    return None


class Stage4AssetsTests(unittest.TestCase):
    def test_hql_tables_defined(self) -> None:
        text = (ROOT / "sql" / "stage4_ml_dashboard.hql").read_text(encoding="utf-8")
        self.assertNotIn("OpenCSVSSerde", text)
        for name in (
            "ml_feature_catalog",
            "ml_hyperparam_grid",
            "ml_eval_metrics",
            "ml_pred_random_forest",
            "ml_pred_linear_svc",
            "ml_pred_naive_bayes",
        ):
            self.assertRegex(text, rf"\b{name}\b")

    def test_stage4_paths_align_with_stage3(self) -> None:
        text = (ROOT / "sql" / "stage4_ml_dashboard.hql").read_text(encoding="utf-8")
        self.assertIn("/user/team14/project/output/evaluation.csv", text)
        self.assertIn("/user/team14/project/output/model1_predictions", text)

    def test_catalog_csv_headers(self) -> None:
        feat = ROOT / "sql" / "data" / "stage4_ml_features.csv"
        with feat.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.reader(fh))
        self.assertEqual(rows[0], ["feature_name", "feature_type", "processing"])
        self.assertTrue(all(len(r) == 3 for r in rows[1:]))

        hyp = ROOT / "sql" / "data" / "stage4_ml_hyper.csv"
        with hyp.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.reader(fh))
        self.assertEqual(
            rows[0],
            ["model", "parameter", "search_space", "best_selection_note"],
        )
        self.assertTrue(all(len(r) == 4 for r in rows[1:]))

    def test_stage4_shell_syntax(self) -> None:
        bash = _working_bash()
        if bash is None:
            self.skipTest("no working bash for -n check")
        script = ROOT / "scripts" / "stage4.sh"
        subprocess.run([bash, "-n", str(script)], check=True)

    def test_stage4_script_requires_secret_file_check(self) -> None:
        body = (ROOT / "scripts" / "stage4.sh").read_text(encoding="utf-8")
        self.assertIn("secrets/.hive.pass", body)


if __name__ == "__main__":
    unittest.main()
