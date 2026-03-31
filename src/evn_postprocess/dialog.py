import abc
import sys
from rich import print as rprint
from rich.panel import Panel
from rich.console import Console
from loguru import logger
from . import experiment
from . import utils

_console = Console()

class Dialog(object, metaclass=abc.ABCMeta):
    """Abstract class that implements the basic functionality for any
    User Interface required for the post-processing.
    """
    @abc.abstractmethod
    def askMSoperations(self, exp):
        """Dialog that requests the following parameters in order to process the MS
        of a given experiment:

        - Weight threshold for the flagging in the MS. A float number between 0 and 1.0.
        - Antennas that require a polswap.
        - Antennas that recorded one-bit data and require the conversion to two-bit.
        - Antennas that require to run PolConvert because they recorded linear polarization.

        These parameters need to be loaded into the respective parameters inside the exp
        object (passed to the function).
        
        Args:
            exp (experiment.Experiment): Experiment object to update with user-provided parameters.
        
        Returns:
            bool: True if the dialog and recording of the parameters went successfully.
        """
        raise NotImplementedError('users must define this function to use this base class')


class Terminal(Dialog):

    def _styled_input(self, label: str, hint: str = "") -> str:
        """Prints a Rich-styled prompt label, then reads raw input on the next line.

        Args:
            label: Bold prompt text (Rich markup allowed).
            hint: Optional dim hint shown below the label.

        Returns:
            The stripped user input string.
        """
        rprint(f"\n  [bold cyan]{label}[/bold cyan]")
        if hint:
            rprint(f"  [dim]{hint}[/dim]")
        return input("  > ").strip()

    def ask_for_antennas(self, exp, label: str, hint: str = ""):
        """Asks for a list of antennas and parses them.
        It verifies that all introduced antennas are included in the experiment.

        Args:
            exp (experiment.Experiment): Experiment object containing valid antenna names.
            label (str): Bold prompt text (Rich markup allowed).
            hint (str): Optional dim hint shown below the label.

        Returns:
            list[str]: List of antenna names provided by the user, or empty list if none specified.
        """
        antennas = []
        while True:
            try:
                output = self._styled_input(label, hint).replace('\n', '')
                if output != '':
                    antennas = [ant.strip().capitalize() for ant in output.split(',' if ',' in output else ' ')]
                    for antenna in antennas:
                        if antenna not in exp.antennas.names:
                            raise ValueError(f"Antenna {antenna} not recognized (not included "
                                             f"in {', '.join(exp.antennas.names)})")
                break
            except ValueError as e:
                rprint(f"  [bold red]ValueError:[/bold red] [red]{e}[/red]")
                continue
            except KeyboardInterrupt:
                rprint('\n[bold red]Pipeline aborted![/bold red]')
                sys.exit(1)

        return antennas

    def askMSoperations(self, exp):
        """Dialog that requests the following parameters in order to process the MS
        of a given experiment:

        - Weight threshold for the flagging in the MS. A float number between 0 and 1.0.
        - Antennas that require a polswap.
        - Antennas that recorded one-bit data and require the conversion to two-bit.
        - Antennas that require to run PolConvert because they recorded linear polarization.

        These parameters are loaded into the respective parameters inside the exp
        object (passed to the function).
        
        Args:
            exp (experiment.Experiment): Experiment object to update with user-provided parameters.
        
        Returns:
            bool: True if the dialog and recording of the parameters went successfully.
        """
        # Check if all antennas have >95% of data in first or last weight interval (<0.001 or >0.9)
        low_weight_antennas = []
        for ant in exp.antennas:
            if (total_data := sum(ant.weights)) > 0:
                if ((ant.weights[0] + ant.weights[6]) / total_data) < 0.95 or (ant.weights[6] == 0):
                    low_weight_antennas.append(ant.name)

        ant_list = ', '.join(exp.antennas.names)
        _console.print(Panel("[bold]Review the standard plots and answer the following questions.[/bold]\n"
                             f"Available antennas: [cyan]{ant_list}[/cyan]",
                             title="[bold yellow]MS Operations[/bold yellow]", border_style="yellow", padding=(1, 2)))

        if low_weight_antennas:
            rprint(f"  [bold yellow]Warning:[/bold yellow] [yellow]{', '.join(low_weight_antennas)} "
                   "show unexpectedly low weights — check the weight plots.[/yellow]")
            while True:
                try:
                    threshold = float(self._styled_input("Threshold for flagging weights in the MS",
                                                        "Float between 0.0 and 1.0"))
                    if 0.0 < threshold < 1.0:
                        break
                    else:
                        rprint("  [red]The threshold needs to be a value within (0.0, 1.0).[/red]")
                except ValueError:
                    rprint('  [bold red]ValueError:[/bold red] [red]Could not convert input to float.[/red]')
                    continue
        else:
            rprint("  [dim]Weight threshold automatically set to 0.9 (weights look fine).[/dim]")
            threshold = 0.9

        polswap = self.ask_for_antennas(exp, "Antennas for polswap", "Comma or space separated, leave empty if none")
        if utils.station_1bit_in_vix(exp.vixfile):
            onebit = self.ask_for_antennas(exp, "Antennas that recorded one-bit data")
        else:
            onebit = []

        polconvert = self.ask_for_antennas(exp, "Antennas that require PolConvert",
                                           "Linear-pol antennas to convert, leave empty if none")

        for i in range(len(exp.correlator_passes)):
            existing = exp.correlator_passes[i].flagged_weights
            if existing and existing.threshold == threshold and existing.percentage >= 0:
                logger.info(f"flag_weights threshold unchanged ({threshold}) for "
                            f"{exp.correlator_passes[i].msfile.name}, keeping previous result.")
            else:
                exp.correlator_passes[i].flagged_weights = experiment.FlagWeight(threshold, -1)

        for antenna in polswap:
            exp.antennas[antenna].polswap = True

        for antenna in polconvert:
            exp.antennas[antenna].polconvert = True

        for antenna in onebit:
            exp.antennas[antenna].onebit = True

        return True

    def show_scan_overview(self, exp: experiment.Experiment) -> bool:
        """Displays a terminal-based table showing scan participation for each antenna.

        Uses exp.scans (already populated from VEX + MS metadata) so no files are re-read.
        Cells are colored:
        - Green: Antenna has data for that scan
        - Red: Antenna was scheduled but has no data for that scan
        - No color: Antenna was not scheduled in that scan

        Scan number and source name are colored by source type:
        - Orange: Fringe-finder
        - Cyan: Target
        - Yellow: Phase-cal (calibrator)
        - Dim: Other / unknown

        Args:
            exp (experiment.Experiment): Experiment object with scans already populated.

        Returns:
            bool: True if user wants to continue, False if user cancels.
        """
        from rich.console import Console
        from rich.table import Table
        from rich.text import Text
        from rich.panel import Panel
        import blessed

        if not exp.scans:
            rprint("[yellow]No scan information available. Skipping scan overview.[/yellow]")
            return True

        # Build source name -> type name lookup
        source_type_styles: dict[str, str] = {
            "fringefinder": "bold cyan",
            "target": "bold dark_orange",
            "calibrator": "bold yellow",
            "other": "dim",
        }
        not_observed = {a.name for a in exp.antennas if not a.observed}
        source_type_map: dict[str, str] = {}
        for src in exp.sources:
            source_type_map[src.name] = src.type.name

        term = blessed.Terminal()
        console = Console()

        with term.fullscreen(), term.cbreak():
            table = Table(title=f"Scan Overview - {exp.expname}")
            table.add_column("Scan", no_wrap=True)
            table.add_column("Source", no_wrap=True)

            all_antennas = sorted(exp.antennas.names)
            for antenna in all_antennas:
                hdr_style = "bold red" if antenna in not_observed else None
                table.add_column(antenna, width=4, justify="center", header_style=hdr_style)

            for scan in exp.scans:
                scheduled = set(scan.stations_scheduled)
                observed = set(scan.stations_observed)
                stype = source_type_map.get(scan.source, "other")
                style = source_type_styles.get(stype, "dim")
                row_cells: list = [Text(str(scan.scanno), style=style), Text(scan.source, style=style)]
                for antenna in all_antennas:
                    if antenna in scheduled:
                        if antenna in observed:
                            cell_text = Text("✓", style="bold white on green")
                        else:
                            cell_text = Text("✗", style="bold white on red")
                    else:
                        cell_text = Text("-", style="dim")
                    row_cells.append(cell_text)
                table.add_row(*row_cells)

            legend_text = ("[bold white on green]✓[/bold white on green] Scheduled & Observed  "
                           "[bold white on red]✗[/bold white on red] Scheduled but Missing  "
                           "[dim]-[/dim] Not Scheduled\n"
                           "[bold cyan]■[/bold cyan] Fringe-finder  "
                           "[bold dark_orange]■[/bold dark_orange] Target  "
                           "[bold yellow]■[/bold yellow] Phase-cal  "
                           "[bold red]Antenna[/bold red] Not observed")
            console.print(Panel(table, title="Antenna Scan Participation"))
            console.print()
            console.print(Panel(legend_text, title="Legend"))
            console.print()
            console.print("[bold yellow]Press any key to continue, or 'Q' to cancel...[/bold yellow]")

            with term.cbreak():
                key = term.inkey()
                return key.lower() != 'q'

        return True








