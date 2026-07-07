import tempfile
import subprocess
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from rich import print as rprint
from . import utils
from .experiment import Experiment, Server, Servers, parse_masterprojects  # noqa: F401  (re-exported below)


def get_init_files(expname: str, servers: Servers, eEVNname: str | None = None) -> bool:
    """Retrieves the .vix (or .vox) vex file of the experiment, plus the .piletter.

    The vex file is the only hard requirement (all experiment metadata derives from
    it); the .piletter is fetched best-effort for the later distribution stage and its
    absence only logs a warning. The historical .expsum retrieval is gone: source
    types and PI information now come from the experiment toml.

    Args:
        expname (str): Experiment name.
        servers (Servers): Server configuration objects.
        eEVNname (str | None): Name of the e-EVN run (EXP1) when known; the vex file on
            the correlator server carries that name. Defaults to *expname*: on a first
            e-EVN EXPn initialization the run name is not yet known (it comes from the
            vex itself), so the fetch may fail and the caller should instruct the
            operator to copy the vex from ../EXP1.

    Returns:
        bool: True if the vex file is present locally after the call.
    """
    eEVNname = expname if eEVNname is None else eEVNname
    piletter_server = servers['piletters']
    piletter_path = Path(f"{expname.lower()}.piletter")
    main_vex = Path(f"{expname.upper()}.vix")

    def fetch_piletter():
        if not piletter_path.exists():
            utils.scp(f"{piletter_server.user}@{piletter_server.host}:{piletter_server.path / piletter_path}", '.')
            logger.debug(f"{piletter_path.name} was not found. Retrieved from {piletter_server.host}.")
        else:
            logger.debug(f"{piletter_path.name} already exists")

    def fetch_vix_or_vox():
        ccs_server = servers['ccs']
        base_path = Path(str(ccs_server.path).format(expname=eEVNname))
        remote_host = f"{ccs_server.user}@{ccs_server.host}"
        # Try .vox first, fallback to .vix
        if main_vex.exists():
            logger.debug(f"{expname.upper()}.vix already exists.")
            return True

        for ext in ['vox', 'vix']:
            file_path = Path(f"{eEVNname.lower()}.{ext}")

            if not file_path.exists():
                if utils.remote_file_exists(remote_host, base_path / file_path):
                    utils.scp(f"{remote_host}:{base_path / file_path}", '.')
                    logger.debug(f"{file_path} was not found. Retrieved from {remote_host}.")
                else:
                    continue

            try:
                main_vex.symlink_to(file_path)
                logger.debug(f"Symlink {file_path} -> {main_vex} created.")
            except FileExistsError:
                logger.error(f"{expname.lower()} vix/vox file not found in {remote_host}. "
                             "It may have a non-standard name.")
                return False

            return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {'piletter': executor.submit(fetch_piletter),
                   'vex': executor.submit(fetch_vix_or_vox)}
        try:
            futures['piletter'].result()
        except Exception as e:  # best-effort: only needed at distribution time
            logger.warning(f"Could not retrieve {piletter_path.name} (continuing): {e}")
        futures['vex'].result()

    return main_vex.exists()


def get_vlbeer_sched_files(expname: str, obsdate: dt.date, server: Server) -> bool:
    """Retrieves the .key and .sum observing files from vlbeer.

    Args:
        expname (str): Experiment name.
        obsdate (datetime.date): Observation date.
        server (Server): Server object with vlbeer connection information.

    Returns:
        bool: True if the files were retrieved successfully, False otherwise.
    """
    files = [Path(f"{expname.lower()}.key"), Path(f"{expname.lower()}.sum")]

    def fetch_file(a_file: Path):
        if a_file.exists():
            logger.debug(f"{a_file.name} already exists.")
            return

        try:
            s_formatted = utils.format_remote_path(str(server.path), obsdate=obsdate)
            utils.scp(f"{server.user}@{server.host}:{Path(s_formatted) / a_file}",
                            ".", timeout=120)
            logger.debug(f"Retrieved {a_file.name} from vlbeer")
        except subprocess.TimeoutExpired:
            rprint(f"[bold yellow]Could not retrieve {a_file.name} from vlbeer.[/bold yellow]")
            # Because a zero-sized file will be there
            a_file.unlink(missing_ok=True)
            logger.warning(f"Could not retrieve {a_file.name} from vlbeer (timeout)")
        except ValueError:
            rprint(f"[bold yellow]Could not find {a_file.name} in vlbeer.[/bold yellow]")
            a_file.unlink(missing_ok=True)
            logger.warning(f"Could not find {a_file.name} in vlbeer")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(fetch_file, a_file) for a_file in files]
        for future in futures:
            future.result()

    return all([p.exists() for p in files])


# parse_masterprojects is re-exported from experiment at the top of this module
# (kept for backwards compatibility with historical io.parse_masterprojects call sites).


def get_jexp_info(expname: str, server: Server) -> dict[str, str | None]:
    """Retrieves the information from the jexp file associated to the experiment,
    whose location should be defined in the introduced server.

    Args:
        expname (str): The experiment name (case insensitive).
        server (Server): Server where the jexp file can be found.

    Returns:
        dict[str, str | None]: Dictionary containing all information described in the jexp file.
    """
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.jex')
    temp_file.close()
    try:
        utils.scp(f"{server.user}@{server.host}:" + str(server.path / f"{expname.lower()}.jex"),
                        temp_file.name, capture_output=True)
        with open(temp_file.name, 'r') as f:
            jexp_content = f.read()
    finally:
        Path(temp_file.name).unlink(missing_ok=True)

    result = {}
    for line in jexp_content.strip().split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        # Remove trailing semicolon if present
        if line.endswith(';'):
            line = line[:-1]

        # Split by '=' to get key-value pairs. Lines without '=' are skipped (previously
        # they fell through and re-stored the previous line's key/value, or raised
        # NameError when the first data line had no '=').
        if '=' in line:
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()
            # Set empty values to None
            result[key] = value if value else None

    return result


@dataclass
class StationFeedback:
    station: str
    type: str
    comment: str


def get_station_feedback_info(expname: str, server: Server) -> dict[str, StationFeedback]:
    """Retrieves the information that stations reported in the station feedback database.

    Args:
        expname (str): The experiment name (case insensitive).
        server (Server): Server where the station feedback database can be found.

    Returns:
        dict[str, StationFeedback]: Dictionary containing the station codename as key, and
            the reported information as a StationFeedback class (which contains .station, .type, .comment
            parameters).
    """
    raise NotImplementedError

