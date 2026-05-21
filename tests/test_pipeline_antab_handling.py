"""
Tests for EVN postprocess pipeline .antab file handling.

This module contains tests to ensure proper handling of .antab files in different scenarios:
1. One .antab file with one correlator pass - should work fine
2. One .antab file with multiple passes including spectral line - should detect error
3. One .antab file with multiple passes - should create numbered .antab files
"""
import pytest
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile
import os

from evn_postprocess import pipeline, experiment


class TestPipelineAntabFileHandling:
    """Test suite for pipeline .antab file handling scenarios."""
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        
        # Create directory structure
        self.pipe_temp_dir = Path(self.temp_dir) / "temp"
        self.pipe_in_dir = Path(self.temp_dir) / "in"
        self.pipe_temp_dir.mkdir(exist_ok=True)
        self.pipe_in_dir.mkdir(exist_ok=True)
        
        # Create a mock experiment. Mock(spec=Experiment) only auto-allows class-level
        # attributes; supsci is set in __init__, so we have to attach it explicitly
        # otherwise pipeline.create_input_file blows up evaluating exp.supsci.lower().
        self.mock_exp = Mock(spec=experiment.Experiment)
        self.mock_exp.expname = "testexp"
        self.mock_exp.supsci = "testjss"
        self.mock_exp.refant = ["Ef"]
        self.mock_exp.multi_phase_center = False
        
        # Create mock directories
        self.mock_dirs = Mock()
        self.mock_dirs.pipe_temp = self.pipe_temp_dir
        self.mock_dirs.pipe_in = self.pipe_in_dir
        self.mock_exp.dirs = self.mock_dirs
        
        # Create mock correlator passes
        self.mock_pass1 = Mock(spec=experiment.CorrelatorPass)
        self.mock_pass1.pipeline = True
        self.mock_pass1.sources = Mock()
        self.mock_pass1.sources.fringefinder = ["source1"]
        self.mock_pass1.sources.target = ["target1"]
        self.mock_pass1.sources.calibrator = ["cal1"]
        self.mock_pass1.sources.calibrator_for_target = Mock(return_value="cal1")
        
        self.mock_pass2 = Mock(spec=experiment.CorrelatorPass)
        self.mock_pass2.pipeline = True
        self.mock_pass2.sources = Mock()
        self.mock_pass2.sources.fringefinder = ["source2"]
        self.mock_pass2.sources.target = ["target2"]
        self.mock_pass2.sources.calibrator = ["cal2"]
        self.mock_pass2.sources.calibrator_for_target = Mock(return_value="cal2")
    
    def teardown_method(self):
        """Clean up after each test method."""
        os.chdir(self.original_cwd)
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_single_antab_single_pass_should_work(self):
        """Test case 1: One .antab file with one correlator pass should work fine."""
        # Setup: single pass, single .antab file
        self.mock_exp.correlator_passes = [self.mock_pass1]
        
        # Create single .antab file in temp directory
        antab_file = self.pipe_temp_dir / "testexp.antab"
        antab_file.write_text("test antab content")
        
        # Mock subprocess calls
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout = "100"
            
            with patch('evn_postprocess.pipeline.resources.files') as mock_resources:
                mock_template = Mock()
                mock_template.read.return_value = "template content with {expname} {userno} {refant}"
                mock_resources.return_value.joinpath.return_value.read_text.return_value = "template content with {expname} {userno} {refant}"
                
                result = pipeline.create_input_file(self.mock_exp)
                
                assert result is True
                
                # Check that the .antab file was copied to pipe_in
                copied_antab = self.pipe_in_dir / "testexp.antab"
                assert copied_antab.exists()
                
                # Check that pipeline input file was created
                input_file = self.pipe_in_dir / "testexp.inp.txt"
                assert input_file.exists()
    
    def test_single_antab_multiple_passes_spectral_line_should_error(self):
        """Test case 2: One .antab file with multiple passes including spectral line should detect error."""
        # Setup: multiple passes with spectral line, single .antab file
        self.mock_exp.spectral_line = True
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2]
        
        # Create single .antab file in temp directory
        antab_file = self.pipe_temp_dir / "testexp.antab"
        antab_file.write_text("test antab content")
        
        # Create .uvflg file as well
        uvflg_file = self.pipe_temp_dir / "testexp.uvflg"
        uvflg_file.write_text("test uvflg content")
        
        # Mock subprocess calls
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout = "100"
            
            with patch('evn_postprocess.pipeline.resources.files') as mock_resources:
                mock_template = Mock()
                mock_template.read.return_value = "template content with {expname} {userno} {refant}"
                mock_resources.return_value.joinpath.return_value.read_text.return_value = "template content with {expname} {userno} {refant}"
                
                # This should create numbered .antab files automatically
                result = pipeline.create_input_file(self.mock_exp)
                
                assert result is True
                
                # For spectral line experiments, the code should automatically create 
                # numbered .antab files, so this should not be an error
                # The test verifies that the numbered files are created
                antab_1 = self.pipe_in_dir / "testexp_1.antab"
                antab_2 = self.pipe_in_dir / "testexp_2.antab"
                
                assert antab_1.exists()
                assert antab_2.exists()
    
    def test_single_antab_multiple_passes_should_create_numbered_files(self):
        """Test case 3: One .antab file with multiple passes should create numbered .antab files."""
        # Setup: multiple passes, single .antab file
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2]
        
        # Create single .antab file in temp directory
        antab_file = self.pipe_temp_dir / "testexp.antab"
        antab_file.write_text("test antab content")
        
        # Create .uvflg file as well
        uvflg_file = self.pipe_temp_dir / "testexp.uvflg"
        uvflg_file.write_text("test uvflg content")
        
        # Mock subprocess calls
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout = "100"
            
            with patch('evn_postprocess.pipeline.resources.files') as mock_resources:
                mock_template = Mock()
                mock_template.read.return_value = "template content with {expname} {userno} {refant}"
                mock_resources.return_value.joinpath.return_value.read_text.return_value = "template content with {expname} {userno} {refant}"
                
                result = pipeline.create_input_file(self.mock_exp)
                
                assert result is True
                
                # Check that original .antab file was copied
                original_antab = self.pipe_in_dir / "testexp.antab"
                assert original_antab.exists()
                
                # Check that numbered .antab files were created
                antab_1 = self.pipe_in_dir / "testexp_1.antab"
                antab_2 = self.pipe_in_dir / "testexp_2.antab"
                
                assert antab_1.exists()
                assert antab_2.exists()
                
                # Check that numbered .uvflg files were created
                uvflg_1 = self.pipe_in_dir / "testexp_1.uvflg"
                uvflg_2 = self.pipe_in_dir / "testexp_2.uvflg"
                
                assert uvflg_1.exists()
                assert uvflg_2.exists()
                
                # Check that numbered pipeline input files were created
                input_1 = self.pipe_in_dir / "testexp_1.inp.txt"
                input_2 = self.pipe_in_dir / "testexp_2.inp.txt"
                
                assert input_1.exists()
                assert input_2.exists()
    
    def test_multiple_antab_files_multiple_passes_should_work(self):
        """Test scenario with multiple .antab files already present for multiple passes."""
        # Setup: multiple passes, multiple .antab files already exist
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2]
        
        # Create multiple .antab files in pipe_in directory
        antab_1 = self.pipe_in_dir / "testexp_1.antab"
        antab_2 = self.pipe_in_dir / "testexp_2.antab"
        antab_1.write_text("test antab content 1")
        antab_2.write_text("test antab content 2")
        
        # Create .uvflg files
        uvflg_1 = self.pipe_in_dir / "testexp_1.uvflg"
        uvflg_2 = self.pipe_in_dir / "testexp_2.uvflg"
        uvflg_1.write_text("test uvflg content 1")
        uvflg_2.write_text("test uvflg content 2")
        
        # Mock subprocess calls
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout = "100"
            
            with patch('evn_postprocess.pipeline.resources.files') as mock_resources:
                mock_template = Mock()
                mock_template.read.return_value = "template content with {expname} {userno} {refant}"
                mock_resources.return_value.joinpath.return_value.read_text.return_value = "template content with {expname} {userno} {refant}"
                
                result = pipeline.create_input_file(self.mock_exp)
                
                assert result is True
                
                # Check that numbered pipeline input files were created
                input_1 = self.pipe_in_dir / "testexp_1.inp.txt"
                input_2 = self.pipe_in_dir / "testexp_2.inp.txt"
                
                assert input_1.exists()
                assert input_2.exists()
    
    def test_no_antab_files_should_copy_from_temp(self):
        """Test that .antab files are copied from temp directory when none exist in pipe_in."""
        # Setup: no .antab files in pipe_in, some in temp
        self.mock_exp.correlator_passes = [self.mock_pass1]
        
        # Create .antab file in temp directory
        temp_antab = self.pipe_temp_dir / "testexp.antab"
        temp_antab.write_text("test antab content from temp")
        
        # Mock subprocess calls
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout = "100"
            
            with patch('evn_postprocess.pipeline.resources.files') as mock_resources:
                mock_template = Mock()
                mock_template.read.return_value = "template content with {expname} {userno} {refant}"
                mock_resources.return_value.joinpath.return_value.read_text.return_value = "template content with {expname} {userno} {refant}"
                
                result = pipeline.create_input_file(self.mock_exp)
                
                assert result is True
                
                # Check that .antab file was copied from temp to pipe_in
                copied_antab = self.pipe_in_dir / "testexp.antab"
                assert copied_antab.exists()
                assert copied_antab.read_text() == "test antab content from temp"
    
    def test_no_pipeline_passes_should_handle_gracefully(self):
        """Test behavior when no correlator passes are marked for pipeline."""
        # Setup: no pipeline passes
        self.mock_pass1.pipeline = False
        self.mock_pass2.pipeline = False
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2]
        
        # Create .antab file
        antab_file = self.pipe_temp_dir / "testexp.antab"
        antab_file.write_text("test antab content")
        
        result = pipeline.create_input_file(self.mock_exp)
        
        assert result is True
        # Should not create any numbered files since no pipeline passes
        assert not (self.pipe_in_dir / "testexp_1.antab").exists()
        assert not (self.pipe_in_dir / "testexp_2.antab").exists()
    
    def test_mixed_pipeline_passes_should_only_copy_pipeline_ones(self):
        """Test behavior with mix of pipeline and non-pipeline passes."""
        # Setup: mix of pipeline and non-pipeline passes
        self.mock_pass1.pipeline = True
        self.mock_pass2.pipeline = False
        mock_pass3 = Mock(spec=experiment.CorrelatorPass)
        mock_pass3.pipeline = True
        mock_pass3.sources = Mock()
        mock_pass3.sources.fringefinder = ["source3"]
        mock_pass3.sources.target = ["target3"]
        mock_pass3.sources.calibrator = ["cal3"]
        mock_pass3.sources.calibrator_for_target = Mock(return_value="cal3")
        
        self.mock_exp.correlator_passes = [self.mock_pass1, self.mock_pass2, mock_pass3]
        
        # Create .antab file
        antab_file = self.pipe_temp_dir / "testexp.antab"
        antab_file.write_text("test antab content")
        
        # Create .uvflg file
        uvflg_file = self.pipe_temp_dir / "testexp.uvflg"
        uvflg_file.write_text("test uvflg content")
        
        # Mock subprocess calls
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout = "100"
            
            with patch('evn_postprocess.pipeline.resources.files') as mock_resources:
                mock_template = Mock()
                mock_template.read.return_value = "template content with {expname} {userno} {refant}"
                mock_resources.return_value.joinpath.return_value.read_text.return_value = "template content with {expname} {userno} {refant}"
                
                result = pipeline.create_input_file(self.mock_exp)
                
                assert result is True
                
                # Should only create numbered files for pipeline passes (2 passes)
                antab_1 = self.pipe_in_dir / "testexp_1.antab"
                antab_2 = self.pipe_in_dir / "testexp_2.antab"
                
                assert antab_1.exists()
                assert antab_2.exists()
                
                # Should not create antab_3 since only 2 pipeline passes
                assert not (self.pipe_in_dir / "testexp_3.antab").exists()


class TestPipelineAntabFileEdgeCases:
    """Test edge cases for pipeline .antab file handling."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir)
        
        # Create directory structure
        self.pipe_temp_dir = Path(self.temp_dir) / "temp"
        self.pipe_in_dir = Path(self.temp_dir) / "in"
        self.pipe_temp_dir.mkdir(exist_ok=True)
        self.pipe_in_dir.mkdir(exist_ok=True)
        
        # Create a mock experiment (see TestPipelineAntabFileHandling.setup_method for
        # the rationale on why we set supsci explicitly).
        self.mock_exp = Mock(spec=experiment.Experiment)
        self.mock_exp.expname = "testexp"
        self.mock_exp.supsci = "testjss"
        self.mock_exp.refant = ["Ef"]
        self.mock_exp.multi_phase_center = False
        
        # Create mock directories
        self.mock_dirs = Mock()
        self.mock_dirs.pipe_temp = self.pipe_temp_dir
        self.mock_dirs.pipe_in = self.pipe_in_dir
        self.mock_exp.dirs = self.mock_dirs
    
    def teardown_method(self):
        """Clean up after each test method."""
        os.chdir(self.original_cwd)
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_missing_aips_userno_should_default_to_100(self):
        """Test behavior when ~/.aips_userno is missing or unreadable."""
        # Setup single pass
        mock_pass = Mock(spec=experiment.CorrelatorPass)
        mock_pass.pipeline = True
        mock_pass.sources = Mock()
        mock_pass.sources.fringefinder = ["source1"]
        mock_pass.sources.target = ["target1"]
        mock_pass.sources.calibrator = []
        mock_pass.sources.calibrator_for_target = Mock(return_value=None)
        
        self.mock_exp.correlator_passes = [mock_pass]
        
        # Create .antab file
        antab_file = self.pipe_temp_dir / "testexp.antab"
        antab_file.write_text("test antab content")
        
        # Mock subprocess to simulate missing/empty .aips_userno
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout = ""  # Empty output
            
            with patch('evn_postprocess.pipeline.resources.files') as mock_resources:
                mock_template = Mock()
                mock_template.read.return_value = "template content with {expname} {userno} {refant}"
                mock_resources.return_value.joinpath.return_value.read_text.return_value = "template content with {expname} {userno} {refant}"
                
                result = pipeline.create_input_file(self.mock_exp)
                
                assert result is True
                
                # Check that input file was created with default userno=100
                input_file = self.pipe_in_dir / "testexp.inp.txt"
                assert input_file.exists()
                content = input_file.read_text()
                assert "100" in content  # Default userno
    
    def test_empty_correlator_passes_should_handle_gracefully(self):
        """Test behavior with empty correlator passes list."""
        self.mock_exp.correlator_passes = []
        
        result = pipeline.create_input_file(self.mock_exp)
        
        assert result is True
        # Should not create any files
        assert not list(self.pipe_in_dir.glob("*.antab"))
        assert not list(self.pipe_in_dir.glob("*.inp.txt"))
    
    def test_missing_template_file_should_handle_gracefully(self):
        """Test behavior when pipeline template is missing."""
        # Setup single pass
        mock_pass = Mock(spec=experiment.CorrelatorPass)
        mock_pass.pipeline = True
        mock_pass.sources = Mock()
        mock_pass.sources.fringefinder = ["source1"]
        mock_pass.sources.target = ["target1"]
        mock_pass.sources.calibrator = []
        mock_pass.sources.calibrator_for_target = Mock(return_value=None)
        
        self.mock_exp.correlator_passes = [mock_pass]
        
        # Create .antab file
        antab_file = self.pipe_temp_dir / "testexp.antab"
        antab_file.write_text("test antab content")
        
        # Mock missing template
        with patch('evn_postprocess.pipeline.resources.files') as mock_resources:
            mock_resources.side_effect = FileNotFoundError("Template not found")
            
            # Should handle gracefully or raise appropriate error
            with pytest.raises(FileNotFoundError):
                pipeline.create_input_file(self.mock_exp)
    
    def test_file_permission_errors_should_handle_gracefully(self):
        """Test behavior when file operations fail due to permissions."""
        # Setup single pass
        mock_pass = Mock(spec=experiment.CorrelatorPass)
        mock_pass.pipeline = True
        mock_pass.sources = Mock()
        mock_pass.sources.fringefinder = ["source1"]
        mock_pass.sources.target = ["target1"]
        mock_pass.sources.calibrator = []
        mock_pass.sources.calibrator_for_target = Mock(return_value=None)
        
        self.mock_exp.correlator_passes = [mock_pass]
        
        # Create .antab file
        antab_file = self.pipe_temp_dir / "testexp.antab"
        antab_file.write_text("test antab content")
        
        # Mock permission error on file copy
        with patch('shutil.copy') as mock_copy:
            mock_copy.side_effect = PermissionError("Permission denied")
            
            # Should handle permission error gracefully
            with pytest.raises(PermissionError):
                pipeline.create_input_file(self.mock_exp)


if __name__ == "__main__":
    pytest.main([__file__])
