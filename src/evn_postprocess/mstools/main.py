#!/usr/bin/env python3
"""Main CLI entry point for mstools."""
import sys
import traceback
import argparse
from rich.console import Console
from rich_argparse import RichHelpFormatter
from . import misc, mounts, msdata, operations

console = Console()


def create_parser():
    """Create the main argument parser with all subcommands.
    
    Returns:
        argparse.ArgumentParser: Configured parser with overview and run subcommands.
    """
    parser = argparse.ArgumentParser(prog='mstools', description='Tools to work with Measurement Set (MS) files',
                                     formatter_class=RichHelpFormatter)
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    overview_parser = subparsers.add_parser('view', help='Display MS file overview',
                                            formatter_class=RichHelpFormatter)
    overview_parser.add_argument('msfile', type=str, help='Path to the MS file')
    overview_parser.add_argument('-s', '--stats', action='store_true', default=False,
                                 help='Checks which antennas actually observed (slower)')

    run_parser = subparsers.add_parser('run', help='Run a specific tool on MS file',
                                       formatter_class=RichHelpFormatter)
    run_subparsers = run_parser.add_subparsers(dest='tool', help='Available tools')
    
    polswap_parser = run_subparsers.add_parser('polswap', help='Swap polarizations for specified antennas',
                                               formatter_class=RichHelpFormatter)
    polswap_parser.add_argument('msfile', type=str, help='MS file to process')
    polswap_parser.add_argument('antenna', type=str, help='Antenna name to swap polarizations')
    polswap_parser.add_argument('-t1', '--starttime', type=str, default=None,
                                help='Start time (YYYY/MM/DD/hh:mm:ss or YYYY/DOY/hh:mm:ss)')
    polswap_parser.add_argument('-t2', '--endtime', type=str, default=None,
                                help='End time (YYYY/MM/DD/hh:mm:ss or YYYY/DOY/hh:mm:ss)')
    
    copypol_parser = run_subparsers.add_parser('copypol', help='Copy data from one polarization to another',
                                               formatter_class=RichHelpFormatter)
    copypol_parser.add_argument('msfile', type=str, help='MS file to process')
    copypol_parser.add_argument('antenna', type=str, help='Antenna name')
    copypol_parser.add_argument('polfrom', type=str, choices=['R', 'L', 'X', 'Y', 'r', 'l', 'x', 'y'],
                                help='Polarization to copy from')
    
    scale1bit_parser = run_subparsers.add_parser('scale1bit', help='Scale 1-bit data for quantization correction',
                                                 formatter_class=RichHelpFormatter)
    scale1bit_parser.add_argument('msfile', type=str, help='MS file to process')
    scale1bit_parser.add_argument('antenna', type=str, nargs='+', help='Antenna name(s)')
    scale1bit_parser.add_argument('--undo', action='store_true', help='Undo the scaling')
    scale1bit_parser.add_argument('--no-scale-weights', action='store_false', dest='scale_weights',
                                  help='Do not scale weights')
    
    invertsubband_parser = run_subparsers.add_parser('invert_subband', help='Invert frequency subbands for antenna',
                                                     formatter_class=RichHelpFormatter)
    invertsubband_parser.add_argument('msfile', type=str, help='MS file to process')
    invertsubband_parser.add_argument('antenna', type=str, nargs='+', help='Antenna name(s)')
    invertsubband_parser.add_argument('-t1', '--starttime', type=str, default=None, help='Start time')
    invertsubband_parser.add_argument('-t2', '--endtime', type=str, default=None, help='End time')
    
    flagweights_parser = run_subparsers.add_parser('flag_weights', help='Flag data based on weight threshold',
                                                   formatter_class=RichHelpFormatter)
    flagweights_parser.add_argument('msfile', type=str, help='MS file to process')
    flagweights_parser.add_argument('threshold', type=float, help='Weight threshold (0-1)')
    flagweights_parser.add_argument('--no-apply', action='store_false', dest='apply',
                                    help='Dry run, do not apply flags')
    
    changeproject_parser = run_subparsers.add_parser('expname', help='Change the project name in the MS',
                                                     formatter_class=RichHelpFormatter)
    changeproject_parser.add_argument('msfile', type=str, help='MS file to modify')
    changeproject_parser.add_argument('new_name', type=str, help='New project name')

    changesrc_parser = run_subparsers.add_parser('srcname', help='Change the name of a specific source in the MS',
                                                 formatter_class=RichHelpFormatter)
    changesrc_parser.add_argument('msfile', type=str, help='MS file to modify')
    changesrc_parser.add_argument('src_name', type=str, help='Current source name')
    changesrc_parser.add_argument('new_name', type=str, help='New source name')
    
    printmounts_parser = run_subparsers.add_parser('print_mounts', help='Print antenna mount information',
                                                   formatter_class=RichHelpFormatter)
    printmounts_parser.add_argument('msfile', type=str, help='MS file to read')
    
    modifymounts_parser = run_subparsers.add_parser('modify_mounts', help='Modify antenna mount type',
                                                    formatter_class=RichHelpFormatter)
    modifymounts_parser.add_argument('msfile', type=str, help='MS file to modify')
    modifymounts_parser.add_argument('antenna', type=str, help='Antenna name')
    modifymounts_parser.add_argument('mount', type=str, help='Mount type (e.g., ALT-AZ, EQUATORIAL)')
    
    fixyebes_parser = run_subparsers.add_parser('ysfocus', help='Fix Yebes antenna mount type',
                                                formatter_class=RichHelpFormatter)
    fixyebes_parser.add_argument('msfile', type=str, help='MS file to modify')
    
    fixhobart_parser = run_subparsers.add_parser('hofocus', help='Fix Hobart antenna mount type',
                                                 formatter_class=RichHelpFormatter)
    fixhobart_parser.add_argument('msfile', type=str, help='MS file to modify')
    return parser


def main():
    """Main CLI entry point for mstools command.
    
    Parses command-line arguments and dispatches to appropriate tool function.
    Handles overview command and run subcommands (polswap, mssplit, add_intent, print_mounts).
    """
    parser = create_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)
    
    try:
        if args.command == 'view':
            console.print(f"[bold cyan]Displaying overview of {args.msfile}[/bold cyan]")
            msdata.Ms(args.msfile, args.stats).overview()
        elif args.command == 'run':
            if not args.tool:
                console.print("[bold red]Error:[/bold red] No tool specified. Use 'mstools run <tool>'")
                sys.exit(1)
            
            match args.tool:
                case 'polswap':
                    operations.polswap(args.msfile, args.antenna, misc.parse_time(args.starttime),
                                       misc.parse_time(args.endtime))
                case 'copypol':
                    operations.copy_pol(args.msfile, args.antenna, args.polfrom)
                case 'scale1bit':
                    operations.scale1bit(args.msfile, args.antenna, args.undo, args.scale_weights)
                case 'invert_subband':
                    operations.invert_subband(args.msfile, args.antenna, misc.parse_time(args.starttime),
                                              misc.parse_time(args.endtime))
                case 'flag_weights':
                    operations.flag_weights(args.msfile, args.threshold, args.apply)
                case 'expname':
                    operations.change_project_name(args.msfile, args.new_name)
                case 'srcname':
                    operations.change_source_name(args.msfile, args.src_name, args.new_name)
                case 'print_mounts':
                    mounts.print_mounts(args.msfile)
                case 'modify_mounts':
                    mounts.modify_mounts(args.msfile, args.antenna, args.mount)
                case 'ysfocus':
                    mounts.fix_yebes_mount(args.msfile)
                case 'hofocus':
                    mounts.fix_hobart_mount(args.msfile)
                case _:
                    console.print(f"[bold red]Option '{args.tool}' does not exist[/bold red]")
                    sys.exit(1)
    except Exception as e:
        traceback.print_tb(sys.exc_info()[2])
        console.print(f"[bold red]ERROR![/bold red]  [red]{str(e)}[/red]")
        sys.exit(1)

if __name__ == '__main__':
    main()