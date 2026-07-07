"""The 'aips' pipeline backend: the EVN.py AIPS pipeline.

Wraps the historical pipeline-module functions unchanged (same files, same commands),
so the generated input files stay identical to the pre-refactor version. Imported only
when the 'aips' backend is selected.
"""
from __future__ import annotations

from . import PipelineBackend


class AipsPipeline(PipelineBackend):
    """The EVN.py AIPS pipeline, as run at JIVE (default backend)."""
    name = 'aips'

    def prepare(self, exp) -> bool:
        """Creates the EVN pipeline input file(s) from the local antab/uvflg files."""
        from .. import pipeline
        return pipeline.create_input_file(exp)

    def run(self, exp) -> bool:
        """Runs EVN.py for all correlated passes."""
        from .. import pipeline
        return pipeline.run_pipeline(exp)

    def collect(self, exp) -> bool:
        """Creates the .comment/.tasav files and the pipeline feedback pages."""
        from .. import pipeline
        return pipeline.comment_tasav_files(exp) & pipeline.pipeline_feedback(exp)
