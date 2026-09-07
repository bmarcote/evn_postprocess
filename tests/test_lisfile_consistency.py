"""
Tests for EVN postprocess lis file consistency and robustness.

This module contains tests to ensure that multiple .lis files for a given project
have different names, different output msfile names, and different fitsidinames.
"""
import pytest
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile
import os

from evn_postprocess import lisfiles, experiment


CLEAN_OUTPUT = "First scan = 1\nLast scan = 100"


def clean_checklis_results(number_of_passes: int) -> list:
    """Return value of the parallel checklis run when no .lis file reports any issue.

    Args:
        number_of_passes (int): How many correlator passes were checked.

    Returns:
        list: One (lisfile name, raw output, issues) tuple per pass, with every issue list
        empty.
    """
    return [(f"pass{i}.lis", CLEAN_OUTPUT, {'duplicated': [], 'skipping': [], 'other': []})
            for i in range(number_of_passes)]



class TestLisFileConsistency:
    """Test suite for .lis file consistency checks."""
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        
        # Create a mock experiment
        self.mock_exp = Mock(spec=experiment.Experiment)
        self.mock_exp.expname = "testexp"
        self.mock_exp.spectral_line = False
        
        # Create mock correlator passes
        self.mock_pass1 = Mock(spec=experiment.CorrelatorPass)
        self.mock_pass1.lisfile = Path("testexp1.lis")
        self.mock_pass1.msfile = Path("testexp1.ms")
        self.mock_pass1.fitsidifile = "testexp1_1.IDI"
        
        self.mock_pass2 = Mock(spec=experiment.CorrelatorPass)
        self.mock_pass2.lisfile = Path("testexp2.lis")
        self.mock_pass2.msfile = Path("testexp2.ms")
        self.mock_pass2.fitsidifile = "testexp2_1.IDI"
        
        self.mock_pass3 = Mock(spec=experiment.CorrelatorPass)
        self.mock_pass3.lisfile = Path("testexp3.lis")
        self.mock_pass3.msfile = Path("testexp3.ms")
        self.mock_pass3.fitsidifile = "testexp3_1.IDI"
    
    def teardown_method(self):
        """Clean up after each test method."""
        os.chdir(self.original_cwd)
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_check_lisfiles_all_unique_names_should_pass(self):
        """Test that check_lisfiles passes when all .lis files have unique names."""
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2, self.mock_pass3]

        # We patch ThreadPoolExecutor so the per-pass shell_command never runs;
        # the test only asserts on the post-aggregation uniqueness checks. Asserting
        # that shell_command was called would be testing an implementation detail of
        # the patched-out worker, so we don't.
        with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
            mock_executor.return_value.__enter__.return_value.map.return_value = clean_checklis_results(3)

            result = lisfiles.check_lisfiles(self.mock_exp)

            assert result is True
    
    def test_check_lisfiles_duplicate_lis_names_should_fail(self):
        """Test that check_lisfiles fails when .lis files have duplicate names."""
        # Create passes with duplicate .lis file names
        duplicate_pass = Mock(spec=experiment.CorrelatorPass)
        duplicate_pass.lisfile = Path("testexp1.lis")  # Same as pass1
        duplicate_pass.msfile = Path("testexp4.ms")
        duplicate_pass.fitsidifile = "testexp4_1.IDI"
        
        self.mock_exp.correlator_passes = [self.mock_pass1, duplicate_pass]
        
        # Mock the shell_command to return valid output
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = clean_checklis_results(2)
                
                # Test the enhanced check_lisfiles function
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is False
    
    def test_check_lisfiles_duplicate_msfile_names_should_fail(self):
        """Test that check_lisfiles fails when msfile names are duplicated."""
        # Create passes with duplicate msfile names
        duplicate_ms_pass = Mock(spec=experiment.CorrelatorPass)
        duplicate_ms_pass.lisfile = Path("testexp4.lis")
        duplicate_ms_pass.msfile = Path("testexp1.ms")  # Same as pass1
        duplicate_ms_pass.fitsidifile = "testexp4_1.IDI"
        
        self.mock_exp.correlator_passes = [self.mock_pass1, duplicate_ms_pass]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = clean_checklis_results(2)
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is False
    
    def test_check_lisfiles_duplicate_fitsidinames_should_fail(self):
        """Test that check_lisfiles fails when fitsidinames are duplicated."""
        # Create passes with duplicate fitsidinames
        duplicate_fits_pass = Mock(spec=experiment.CorrelatorPass)
        duplicate_fits_pass.lisfile = Path("testexp4.lis")
        duplicate_fits_pass.msfile = Path("testexp4.ms")
        duplicate_fits_pass.fitsidifile = "testexp1_1.IDI"  # Same as pass1
        
        self.mock_exp.correlator_passes = [self.mock_pass1, duplicate_fits_pass]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = clean_checklis_results(2)
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is False
    
    def test_check_lisfiles_mixed_duplicates_should_fail(self):
        """Test that check_lisfiles fails when there are multiple types of duplicates."""
        # Create passes with various duplicates
        mixed_pass1 = Mock(spec=experiment.CorrelatorPass)
        mixed_pass1.lisfile = Path("testexp1.lis")  # Duplicate lis name
        mixed_pass1.msfile = Path("testexp4.ms")
        mixed_pass1.fitsidifile = "testexp4_1.IDI"
        
        mixed_pass2 = Mock(spec=experiment.CorrelatorPass)
        mixed_pass2.lisfile = Path("testexp5.lis")
        mixed_pass2.msfile = Path("testexp2.ms")  # Duplicate ms name
        mixed_pass2.fitsidifile = "testexp1_1.IDI"  # Duplicate fits name
        
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2, mixed_pass1, mixed_pass2]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = clean_checklis_results(4)
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is False
    
    def test_check_lisfiles_single_pass_should_pass(self):
        """Test that check_lisfiles passes with a single .lis file."""
        self.mock_exp.correlator_passes = [self.mock_pass1]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = clean_checklis_results(1)
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is True
    
    def test_check_lisfiles_spectral_line_experiment(self):
        """Test check_lisfiles behavior with spectral line experiments."""
        self.mock_exp.spectral_line = True
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2, self.mock_pass3]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = clean_checklis_results(3)
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is True
    
    def test_check_lisfiles_with_checklis_errors(self):
        """Test that check_lisfiles fails when checklis.py reports errors."""
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            # Simulate checklis.py reporting errors
            mock_shell.return_value = "First scan = 1\nError: Missing scan 50\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = \
                [("testexp1.lis", "First scan = 1\nError: Missing scan 50\nLast scan = 100",
                  {'duplicated': [], 'skipping': [], 'other': ["Error: Missing scan 50"]})] \
                + clean_checklis_results(1)
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is False
    
    @patch('evn_postprocess.lisfiles.ThreadPoolExecutor')
    def test_enhanced_check_lisfiles_integration(self, mock_executor):
        """Integration test for the enhanced check_lisfiles function."""
        # Setup: every per-pass check is short-circuited to True via the executor
        # mock, so the test only exercises the uniqueness-validation logic.
        mock_executor.return_value.__enter__.return_value.map.return_value = clean_checklis_results(3)

        # Create experiment with unique names
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2, self.mock_pass3]

        result = lisfiles.check_lisfiles(self.mock_exp)

        assert result is True
        mock_executor.assert_called()
    


class TestLisFileConsistencyEdgeCases:
    """Test edge cases for .lis file consistency checks."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
    
    def teardown_method(self):
        """Clean up after each test method."""
        os.chdir(self.original_cwd)
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_empty_correlator_passes(self):
        """Test behavior with empty correlator passes list."""
        mock_exp = Mock(spec=experiment.Experiment)
        mock_exp.correlator_passes = []
        mock_exp.spectral_line = False
        
        result = lisfiles.check_lisfiles(mock_exp)
        assert result is True
    
    def test_none_correlator_passes(self):
        """Test behavior with None correlator passes."""
        mock_exp = Mock(spec=experiment.Experiment)
        mock_exp.correlator_passes = None
        mock_exp.spectral_line = False

        # check_lisfiles raises ``TypeError`` because the very first thing it does
        # is ``len(exp.correlator_passes)``. That is acceptable defensive behaviour
        # for the moment — the workflow caller should never pass None — so we
        # accept either TypeError or AttributeError here.
        with pytest.raises((AttributeError, TypeError)):
            lisfiles.check_lisfiles(mock_exp)
    
    def test_case_sensitive_name_comparison(self):
        """Test that name comparison is case sensitive."""
        mock_exp = Mock(spec=experiment.Experiment)
        mock_exp.expname = "testexp"
        mock_exp.spectral_line = False
        
        # Create passes with case-sensitive name differences
        pass1 = Mock(spec=experiment.CorrelatorPass)
        pass1.lisfile = Path("testexp1.lis")
        pass1.msfile = Path("testexp1.ms")
        pass1.fitsidifile = "testexp1_1.IDI"
        
        pass2 = Mock(spec=experiment.CorrelatorPass)
        pass2.lisfile = Path("TESTEXP1.LIS")  # Different case
        pass2.msfile = Path("testexp2.ms")
        pass2.fitsidifile = "testexp2_1.IDI"
        
        mock_exp.correlator_passes = [pass1, pass2]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = clean_checklis_results(2)
                
                result = lisfiles.check_lisfiles(mock_exp)
                
                # Should pass because names are case-sensitive and actually different
                assert result is True
    
    def test_whitespace_in_names(self):
        """Test handling of whitespace in file names."""
        mock_exp = Mock(spec=experiment.Experiment)
        mock_exp.expname = "testexp"
        mock_exp.spectral_line = False
        
        # Create passes with whitespace in names
        pass1 = Mock(spec=experiment.CorrelatorPass)
        pass1.lisfile = Path("testexp1.lis")
        pass1.msfile = Path("testexp1.ms")
        pass1.fitsidifile = "testexp1_1.IDI"
        
        pass2 = Mock(spec=experiment.CorrelatorPass)
        pass2.lisfile = Path(" testexp2.lis ")  # Whitespace
        pass2.msfile = Path("testexp2.ms")
        pass2.fitsidifile = "testexp2_1.IDI"
        
        mock_exp.correlator_passes = [pass1, pass2]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = clean_checklis_results(2)
                
                result = lisfiles.check_lisfiles(mock_exp)
                
                # Should pass because whitespace makes them different
                assert result is True
    



def make_pass(name: str) -> Mock:
    """Mock correlator pass whose .lis/MS/FITS-IDI names all derive from `name`."""
    a_pass = Mock(spec=experiment.CorrelatorPass)
    a_pass.lisfile = Path(f"{name}.lis")
    a_pass.msfile = Path(f"{name}.ms")
    a_pass.fitsidifile = f"{name}_1.IDI"
    return a_pass


def make_exp(passes: list) -> Mock:
    """Mock experiment holding the given correlator passes (continuum, not spectral line)."""
    exp = Mock(spec=experiment.Experiment)
    exp.expname = "testexp"
    exp.spectral_line = False
    exp.correlator_passes = passes
    return exp


def checklis_outputs(outputs: dict):
    """side_effect for utils.shell_command returning `outputs[lisfile name]` per pass.

    Args:
        outputs (dict): Maps .lis file name to the raw checklis.py output to fake. Files
            missing from the dict get a clean output (no issues reported).
    """
    def run(command, parameters=None, **kwargs):
        return outputs.get(parameters, "First scan = 1\nLast scan = 100")

    return run


class TestRunChecklis:
    """Test suite for the operator-facing result of running checklis on every pass."""

    def test_no_issues_reports_nothing(self):
        exp = make_exp([make_pass("testexp1"), make_pass("testexp2")])
        with patch('evn_postprocess.utils.shell_command', side_effect=checklis_outputs({})):
            report = lisfiles.run_checklis(exp)

        assert report.all_ok is True
        assert report.details == ''
        assert "passed checklis" in report.headline

    def test_raw_checklis_output_is_always_printed(self, capsys):
        """The operator must always see what checklis returned, one block per .lis file."""
        exp = make_exp([make_pass("testexp1"), make_pass("testexp2")])
        outputs = {"testexp1.lis": "First scan = 1\nLast scan = 42",
                   "testexp2.lis": "First scan = 1\n**** Skipped scan no 34\nLast scan = 100"}
        with patch('evn_postprocess.utils.shell_command', side_effect=checklis_outputs(outputs)):
            lisfiles.run_checklis(exp)

        printed = capsys.readouterr().out
        assert "checklis.py testexp1.lis" in printed
        assert "Last scan = 42" in printed
        assert "checklis.py testexp2.lis" in printed
        assert "**** Skipped scan no 34" in printed
        # ... and the summary comes after the raw output of every .lis file.
        assert printed.index("**** Skipped scan no 34") < printed.index("skipped scans")

    def test_skipped_scans_tolerated_in_multi_phase_center(self):
        """Several passes: skipped scans are expected, so they only warn (all_ok stays True)."""
        exp = make_exp([make_pass("testexp1"), make_pass("testexp2"), make_pass("testexp3")])
        skipped = "First scan = 1\n**** Skipped scan no 34\nLast scan = 100"
        outputs = {"testexp1.lis": skipped, "testexp3.lis": skipped}
        with patch('evn_postprocess.utils.shell_command', side_effect=checklis_outputs(outputs)):
            report = lisfiles.run_checklis(exp)

        assert report.all_ok is True
        assert "skipped scans" in report.details
        assert "double check the file(s) manually" in report.details
        assert "2 .lis file(s): testexp1.lis, testexp3.lis" in report.details
        assert "Please verify the .lis file(s) to see if they are OK" in report.headline

    def test_skipped_scans_fail_in_single_pass_experiment(self):
        exp = make_exp([make_pass("testexp1")])
        outputs = {"testexp1.lis": "First scan = 1\n**** Skipped scan no 34\nLast scan = 100"}
        with patch('evn_postprocess.utils.shell_command', side_effect=checklis_outputs(outputs)):
            report = lisfiles.run_checklis(exp)

        assert report.all_ok is False
        assert "skipped scans" in report.details
        assert "skipped scans in 1 .lis file(s)" in report.headline

    def test_duplicated_data_always_fails(self):
        exp = make_exp([make_pass("testexp1"), make_pass("testexp2")])
        outputs = {"testexp2.lis": "First scan = 1\nDuplicated data for scan 12\nLast scan = 100"}
        with patch('evn_postprocess.utils.shell_command', side_effect=checklis_outputs(outputs)):
            report = lisfiles.run_checklis(exp)

        assert report.all_ok is False
        assert "duplicated data" in report.details
        assert "MUST be fixed manually" in report.details
        assert "1 .lis file(s): testexp2.lis" in report.details
        assert "Please verify the .lis file(s) to see if they are OK" in report.headline

    def test_other_errors_are_treated_as_errors(self):
        exp = make_exp([make_pass("testexp1"), make_pass("testexp2")])
        outputs = {"testexp1.lis": "First scan = 1\nNo scans in the .lis file\nLast scan = 100"}
        with patch('evn_postprocess.utils.shell_command', side_effect=checklis_outputs(outputs)):
            report = lisfiles.run_checklis(exp)

        assert report.all_ok is False
        assert "other errors" in report.details

    def test_checklis_failure_is_reported_as_an_error(self):
        """A checklis.py that cannot even run must never be silently ignored."""
        exp = make_exp([make_pass("testexp1")])
        with patch('evn_postprocess.utils.shell_command', side_effect=ValueError("command not found")):
            report = lisfiles.run_checklis(exp)

        assert report.all_ok is False
        assert "other errors" in report.details

    def test_many_lisfiles_are_summarized(self):
        """Multi-phase-center runs can have dozens of passes: the list of names is truncated."""
        passes = [make_pass(f"testexp{i}") for i in range(20)]
        exp = make_exp(passes)
        duplicated = "First scan = 1\nDuplicated data for scan 12\nLast scan = 100"
        outputs = {a_pass.lisfile.name: duplicated for a_pass in passes}
        with patch('evn_postprocess.utils.shell_command', side_effect=checklis_outputs(outputs)):
            report = lisfiles.run_checklis(exp)

        assert report.all_ok is False
        assert "20 .lis file(s)" in report.details
        assert f"(+{20 - lisfiles._MAX_LISFILES_LISTED} more)" in report.details

    def test_repeated_msfile_names_are_reported(self):
        exp = make_exp([make_pass("testexp1"), make_pass("testexp2")])
        exp.correlator_passes[1].msfile = Path("testexp1.ms")
        with patch('evn_postprocess.utils.shell_command', side_effect=checklis_outputs({})):
            report = lisfiles.run_checklis(exp)

        assert report.all_ok is False
        assert "repeated MS names" in report.details
        assert "testexp1.ms" in report.details

    def test_classify_checklis_output(self):
        issues = lisfiles._classify_checklis_output("First scan = 1\n**** Skipped scan no 3\n"
                                                    "Duplicated data for scan 12\nSomething else broke\n"
                                                    "Last scan = 100\n")
        assert issues['skipping'] == ["**** Skipped scan no 3"]
        assert issues['duplicated'] == ["Duplicated data for scan 12"]
        assert issues['other'] == ["Something else broke"]


if __name__ == "__main__":
    pytest.main([__file__])
