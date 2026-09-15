import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from . import extract_settlement_sbt_truth as extractor
except ImportError:
    import extract_settlement_sbt_truth as extractor


class SettlementInputClassificationTests(unittest.TestCase):
    def test_known_training_guide_is_not_treated_as_statement(self):
        with tempfile.TemporaryDirectory() as directory:
            guide = Path(directory) / "ambiguous-name.pdf"
            statement = Path(directory) / "weekly.pdf"
            guide.write_bytes(b"guide")
            statement.write_bytes(b"statement")

            known_hash = next(iter(extractor.REFERENCE_PDF_SHA256))
            with patch.object(
                extractor,
                "_file_sha256",
                side_effect=lambda path: known_hash if path == guide else "unknown",
            ):
                statements, references = extractor._partition_statement_pdfs([guide, statement])

        self.assertEqual(statements, [statement])
        self.assertEqual(references[0][0], guide)
        self.assertIn("training guide", references[0][1])

    def test_unknown_pdf_is_kept_for_strict_statement_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            unknown = Path(directory) / "unknown.pdf"
            unknown.write_bytes(b"not allowlisted")
            statements, references = extractor._partition_statement_pdfs([unknown])

        self.assertEqual(statements, [unknown])
        self.assertEqual(references, [])


if __name__ == "__main__":
    unittest.main()
