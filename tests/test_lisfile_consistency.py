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
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = [True, True, True]
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is True
                mock_shell.assert_called()
    
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
                mock_executor.return_value.__enter__.return_value.map.return_value = [True, True]
                
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
                mock_executor.return_value.__enter__.return_value.map.return_value = [True, True]
                
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
                mock_executor.return_value.__enter__.return_value.map.return_value = [True, True]
                
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
                mock_executor.return_value.__enter__.return_value.map.return_value = [True, True, True, True]
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is False
    
    def test_check_lisfiles_single_pass_should_pass(self):
        """Test that check_lisfiles passes with a single .lis file."""
        self.mock_exp.correlator_passes = [self.mock_pass1]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = [True]
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is True
    
    def test_check_lisfiles_spectral_line_experiment(self):
        """Test check_lisfiles behavior with spectral line experiments."""
        self.mock_exp.spectral_line = True
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2, self.mock_pass3]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            mock_shell.return_value = "First scan = 1\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = [True, True, True]
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is True
    
    def test_check_lisfiles_with_checklis_errors(self):
        """Test that check_lisfiles fails when checklis.py reports errors."""
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2]
        
        with patch('evn_postprocess.utils.shell_command') as mock_shell:
            # Simulate checklis.py reporting errors
            mock_shell.return_value = "First scan = 1\nError: Missing scan 50\nLast scan = 100"
            
            with patch('evn_postprocess.lisfiles.ThreadPoolExecutor') as mock_executor:
                mock_executor.return_value.__enter__.return_value.map.return_value = [False, True]
                
                result = lisfiles.check_lisfiles(self.mock_exp)
                
                assert result is False
    
    @patch('evn_postprocess.lisfiles.ThreadPoolExecutor')
    @patch('evn_postprocess.utils.shell_command')
    def test_enhanced_check_lisfiles_integration(self, mock_shell, mock_executor):
        """Integration test for the enhanced check_lisfiles function."""
        # Setup mocks
        mock_shell.return_value = "First scan = 1\nLast scan = 100"
        mock_executor.return_value.__enter__.return_value.map.return_value = [True, True, True]
        
        # Create experiment with unique names
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2, self.mock_pass3]
        
        # Test the enhanced function
        result = lisfiles.check_lisfiles(self.mock_exp)
        
        assert result is True
        mock_shell.assert_called()
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
        
        # This should handle the None case gracefully
        with pytest.raises(AttributeError):
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
                mock_executor.return_value.__enter__.return_value.map.return_value = [True, True]
                
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
                mock_executor.return_value.__enter__.return_value.map.return_value = [True, True]
                
                result = lisfiles.check_lisfiles(mock_exp)
                
                # Should pass because whitespace makes them different
                assert result is True
    


if __name__ == "__main__":
    pytest.main([__file__])
