import tempfile
import subprocess
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from rich import print as rprint
from . import utils, experiment


def get_init_files(exp: experiment.Experiment, servers: experiment.Servers) -> bool:
    """Retrieves the files related to this experiment as .vix (or .vox), .piletter and .expsum.
    
    Args:
        exp (experiment.Experiment): Experiment object containing experiment metadata.
        servers (experiment.Servers): Server configuration objects.

    Returns:
        bool: True if the files were retrieved successfully, False otherwise.
    """
    eEVNname = exp.expname if exp.eEVNname is None else exp.eEVNname
    piletter_server = servers['piletter']
    piletter_path = piletter_server.path / f"{exp.expname.lower()}.piletter"
    expsum_path = piletter_server.path / f"{exp.expname.lower()}.expsum"
    def fetch_piletter():
        if not piletter_path.exists():
            utils.scp(f"{piletter_server.user}@{piletter_server.host}:{piletter_path}", '.')
    
    def fetch_expsum():
        if not expsum_path.exists():
            utils.scp(f"{piletter_server.user}@{piletter_server.host}:{expsum_path}", '.')
    
    def fetch_vix_or_vox():
        ccs_server = servers['ccs']
        base_path = Path(str(ccs_server.path).format(expname=eEVNname))
        remote_host = f"{ccs_server.user}@{ccs_server.host}"
        # Try .vox first, fallback to .vix
        for ext in ['vox', 'vix']:
            file_path = base_path / f"{eEVNname.lower()}.{ext}"
            if any(file_path.exists()):
                break

            if utils.remote_file_exists(remote_host, file_path):

                utils.scp(f"{remote_host}:{file_path}", '.')
                break
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(fetch_piletter),
            executor.submit(fetch_expsum),
            executor.submit(fetch_vix_or_vox)
        ]
        for future in futures:
            future.result()
    
    for ext in ('vox', 'vix'):
        if (apath := Path(f"{eEVNname.lower()}.{ext}")).exists():
            apath.symlink_to(f"{exp.expname.upper()}.vix")
            break

    return all([piletter_path.exists(), expsum_path.exists(), Path(f"{exp.expname.upper()}.vix").exists()])


def get_vlbeer_files(expname: str, obsdate: dt.date, server: experiment.Server) -> bool:
    """Retrieves the .key and .sum observing files from vlbeer.
    
    Args:
        expname (str): Experiment name.
        obsdate (datetime.date): Observation date.
        server (experiment.Server): Server object with vlbeer connection information.

    Returns:
        bool: True if the files were retrieved successfully, False otherwise.
    """
    files = [Path(f"{expname.lower()}.key"), Path(f"{expname.lower()}.sum")]
    
    def fetch_file(a_file: Path):
        try:
            utils.scp(f"{server.user}@{server.host}:{Path(str(server.path).format(obsdate=obsdate)) / a_file}",
                            ".", timeout=120)
        except subprocess.TimeoutExpired:
            rprint(f"\n[bold yellow]Could not retrieve {a_file.name} from vlbeer.[/bold yellow]")
            # Because a zero-sized file will be there
            a_file.unlink(missing_ok=True)
        except ValueError:
            rprint(f"\n[bold yellow]Could not find {a_file.name} in vlbeer.[/bold yellow]")
            a_file.unlink(missing_ok=True)
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(fetch_file, a_file) for a_file in files]
        for future in futures:
            future.result()

    return all([p.exists() for p in files])


def parse_masterprojects(expname: str, server: experiment.Server) -> tuple[str, str | None]:
        """Obtains the observing epoch from the file in the server (traditionally MASTER_PROJECTS.LIS).
        In case of being an e-EVN experiment, it will add that information.

        The expected file should be a text file with one line per experiment, with expname (capital case) in the first
        column, followed by the observing epoch (YYMMDD format) in the second column.
        If the entry refers to an e-EVN observation (with multiple experiments in the same run), then it will have
        extra columns indicating all experiments within the run.

        Each of the extra columns will have the experiment name in the first column in a different line,
        followed again by the observing epoch.
        
        Args:
            expname (str): Experiment name to search for.
            server (experiment.Server): Server object with MASTER_PROJECTS.LIS location.
        
        Returns:
            tuple[str, str | None]:
                - The observing epoch of the experiment (YYMMDD format).
                - The e-EVN name if it is an e-EVN experiment, None otherwise.
        """
        process = subprocess.Popen(["ssh", f"{server.user}@{server.host}", f"grep {expname} {server.path}"], shell=False, 
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = process.communicate()[0].decode('utf-8')
        if process.returncode != 0:
            raise ValueError(f"Errorcode {process.returncode} when reading MASTER_PROJECTS.LIS."
                             + f"\n{expname} is probably not in the EVN database.")

        if output.count('\n') == 2:
            # It is an e-EVN experiment!
            # One line will have EXP EPOCH.
            # The other one eEXP EPOCH EXP1 EXP2..
            inputs = [i.split() for i in output[:-1].split('\n')]
            obsdate = ''
            for an_input in inputs:
                if an_input[0] == expname:
                    obsdate = an_input[1]
                else:
                    # The first element is the expname of the e-EVN run
                    eEVNname = an_input[0]

            obsdate = obsdate[2:]
        elif output.count('\n') == 1:
            expline = output[:-1].split()
            if len(expline) > 2:
                # This is an e-EVN, this experiment was the first one (so e-EVN is called the same)
                eEVNname = expline[0].strip()
            else:
                eEVNname = None

            obsdate = expline[1].strip()[2:]
        else:
            raise ValueError(f"{expname} not found in (ccs) MASTER_PROJECTS.LIS or server not reachable.")
        
        return obsdate, eEVNname


def get_jexp_info(expname: str, server: experiment.Server) -> dict[str, str | None]:
    """Retrieves the information from the jexp file associated to the experiment,
    whose location should be defined in the introduced server.

    Args:
        expname (str): The experiment name (case insensitive).
        server (experiment.Server): Server where the jexp file can be found.

    Returns:
        dict[str, str | None]: Dictionary containing all information described in the jexp file.
    """
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.jex')
    utils.scp(f"{server.user}@{server.host}:" + str(server.path / f"{expname.lower()}.jex"), 
                    temp_file.name)
    with open(temp_file.name, 'r') as f:
        jexp_content = f.read()

    result = {}
    for line in jexp_content.strip().split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
    
        # Remove trailing semicolon if present
        if line.endswith(';'):
            line = line[:-1]
    
        # Split by '=' to get key-value pairs
        if '=' in line:
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()
        # Set empty values to empty string
        result[key] = value if value else None

    return result


@dataclass
class StationFeedback:
    station: str
    type: str
    comment: str


def get_station_feedback_info(expname: str, server: experiment.Server) -> dict[str, StationFeedback]:
    """Retrieves the information that stations reported in the station feedback database.

    Args:
        expname (str): The experiment name (case insensitive).
        server (experiment.Server): Server where the station feedback database can be found.

    Returns:
        dict[str, StationFeedback]: Dictionary containing the station codename as key, and
            the reported information as a StationFeedback class (which contains .station, .type, .comment
            parameters).
    """
    raise NotImplementedError

