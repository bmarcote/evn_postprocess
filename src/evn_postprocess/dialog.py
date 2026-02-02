import abc
import sys
from rich import print as rprint
from . import experiment
from . import utils
from . import vex
from . import mstools 

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

    def ask_for_antennas(self, exp, asking_text):
        """Asks for a list of antennas and parses them.
        It verifies that all introduced antennas are included in the experiment.
        
        Args:
            exp (experiment.Experiment): Experiment object containing valid antenna names.
            asking_text (str): Text prompt to display to the user.
        
        Returns:
            list[str]: List of antenna names provided by the user, or empty list if none specified.
        """
        antennas = []
        while True:
            try:
                output = input(asking_text).replace('\n', '')
                if output != '':
                    antennas = [ant.strip().capitalize() for ant in \
                                output.split(',' if ',' in output else ' ')]
                    for antenna in antennas:
                        if antenna not in exp.antennas.names:
                            raise ValueError(f"Antenna {antenna} not recognized (not included "
                                             f"in {', '.join(exp.antennas.names)})")
                break
            except ValueError as e:
                rprint(f"[bold red]ValueError:[/bold red] [red]{e}[/red]")
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
    
        rprint("\n\n\n[bold]Please answer to the following questions:[/bold]\n")

        if low_weight_antennas:
            rprint("[bold yellow]Check weight plots[/bold yellow]"
                   f"[yellow]The antennas {', '.join(low_weight_antennas)} show unexpectedly low weights.[/yellow]\n")
            while True:
                try:
                    threshold = float(input("\n\033[1mThreshold for flagging weights in the MS:\n>\033[0m "))
                    if 0.0 < threshold < 1.0:
                        break
                    else:
                        rprint("[red]The threshold needs to be a value within [0.0, 1.0)[/red].")
                except ValueError:
                    rprint('[bold red]ValueError:[/bold red] [red]could not convert input to float (for threshold).[/red]')
                    continue
        else:
            rprint("Weight threshold automatically set to 0.9 in view of the weights in the data.")
            threshold = 0.9

        polswap = self.ask_for_antennas(exp, "\n\033[1mAntennas for polswap (comma or " \
                                        "Fspace separated)\n\033[0m(possible antennas are: "
                                             f"{', '.join(exp.antennas.names)})\n\033[1m>\033[0m ")
        if utils.station_1bit_in_vix(exp.vixfile):
            onebit = self.ask_for_antennas(exp, "\n\033[1mAntennas that recorded one-bit " \
                                                "data:\n> \033[0m")
        else:
            onebit = []

        polconvert = self.ask_for_antennas(exp, "\n\033[1mAntennas that requires PolConvert" \
                                                ":\n> \033[0m")

        for i in range(len(exp.correlator_passes)):
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
        
        Shows a table with scans as rows and antennas as columns. Cells are colored:
        - Green: Antenna has data for that scan
        - Red: Antenna was scheduled but has no data for that scan
        - No color: Antenna was not scheduled in that scan (per VEX file)
        
        Args:
            exp (experiment.Experiment): Experiment object containing MS and VEX data.
        
        Returns:
            bool: True if user wants to continue, False if user cancels.
        """
        from rich.console import Console
        from rich.table import Table
        from rich.text import Text
        from rich.panel import Panel
        import blessed
        
        # Get VEX scan information
        vex_data = vex.Vex(exp.vixfile)
        vex_scans = {}
        if 'SCHED' in vex_data:
            for scan_name, scan_info in vex_data['SCHED'].items():
                scheduled_antennas = set()
                if 'station' in scan_info:
                    for station_info in scan_info['station']:
                        scheduled_antennas.add(station_info[0])  # station name is first element
                vex_scans[scan_name] = scheduled_antennas
        
        # Get MS scan information
        ms_scans = {}
        for msfile in exp.msfiles:
            try:
                ms = mstools.Ms(msfile, runstats=True)
                for scan_number, antenna_set in ms.scans.items():
                    if scan_number not in ms_scans:
                        ms_scans[scan_number] = set()
                    ms_scans[scan_number].update(antenna_set)
            except Exception as e:
                print(f"Warning: Could not read {msfile}: {e}")
                continue
        
        # Create terminal UI
        term = blessed.Terminal()
        console = Console()
        
        with term.fullscreen(), term.cbreak():
            # Create table
            table = Table(title=f"Scan Overview - {exp.expname}")
            table.add_column("Scan", style="cyan", no_wrap=True)
            
            # Add antenna columns
            all_antennas = sorted(exp.antennas.names)
            for antenna in all_antennas:
                table.add_column(antenna, width=8, justify="center")
            
            # Add rows for each scan
            scan_numbers = sorted(set(list(vex_scans.keys()) + list(ms_scans.keys())))
            
            for scan_num in scan_numbers:
                scan_str = str(scan_num)
                scheduled = vex_scans.get(scan_num, set())
                observed = ms_scans.get(scan_num, set())
                
                row_cells = [scan_str]
                
                for antenna in all_antennas:
                    if antenna in scheduled:
                        if antenna in observed:
                            # Green - scheduled and observed
                            cell_text = Text("✓", style="bold white on green")
                        else:
                            # Red - scheduled but not observed
                            cell_text = Text("✗", style="bold white on red")
                    else:
                        # Not scheduled - no color
                        cell_text = Text("-", style="dim")
                    
                    row_cells.append(cell_text)
                
                table.add_row(*row_cells)
            
            # Create legend
            legend_text = (
                "[bold white on green]✓[/bold white on green] Scheduled & Observed  "
                "[bold white on red]✗[/bold white on red] Scheduled but Missing  "
                "[dim]-[/dim] Not Scheduled"
            )
            
            # Display
            console.print(Panel(table, title="Antenna Scan Participation"))
            console.print()
            console.print(Panel(legend_text, title="Legend"))
            console.print()
            console.print("[bold yellow]Press any key to continue, or 'Q' to cancel...[/bold yellow]")
            
            # Wait for user input
            with term.cbreak():
                key = term.inkey()
                return key.lower() != 'q'
        
        return True








