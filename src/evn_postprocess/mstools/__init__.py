"""Tools subpackage for MS (Measurement Set) file manipulation.

This package provides tools for working with radio astronomy Measurement Set files,
including metadata extraction, data operations, and antenna mount corrections.

Main components:
- misc: Utility functions and Stokes enumeration
- msdata: Classes for MS metadata representation (Ms, Antenna, Source, etc.)
- operations: Data manipulation functions (polswap, scale1bit, flag_weights, etc.)
- mounts: Antenna mount type correction functions
- main: CLI interface for the tools

Example usage:
    >>> from evn_postprocess.mstools import Ms
    >>> ms = Ms('observation.ms')
    >>> ms.overview()  # Display MS information
    >>> ms.operations.polswap('AntennaName')
"""
from . import misc
from .msdata import Ms, ObsEpoch, Source, Sources, Antenna, Antennas, FreqSetup
from .mounts import print_mounts, modify_mounts, fix_yebes_mount, fix_hobart_mount
from .operations import polswap, copy_pol, scale1bit, invert_subband, flag_weights, change_project_name, change_source_name

__all__ = [
    'misc',
    'Ms',
    'ObsEpoch',
    'Source',
    'Sources',
    'Antenna',
    'Antennas',
    'FreqSetup',
    'msoverview',
    'print_mounts',
    'modify_mounts',
    'fix_yebes_mount',
    'fix_hobart_mount',
    'polswap',
    'copy_pol',
    'scale1bit',
    'invert_subband',
    'flag_weights',
    'change_project_name',
    'change_source_name',
]