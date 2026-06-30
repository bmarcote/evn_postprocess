"""Communication module for EVN post-processing notifications.

Supports three modes configured via ``comms.toml``:

- **none** — No notifications (default, current behaviour).
- **email** — Send notifications via SMTP email.
- **mattermost** — Send notifications via Mattermost REST API with optional
  interactive feedback (e.g. msops answers sent back from a DM).

Configuration is loaded from the first file found in the following order:

1. Explicit path (CLI ``--comms`` flag).
2. ``./comms.toml`` in the current working directory.
3. ``$XDG_CONFIG_HOME/evn/comms.toml`` (or ``~/.config/evn/comms.toml``).
4. ``~jops/.config/evn/comms.toml``.

Environment variables ``POSTPROCESS_SMTP_PASSWORD`` and ``POSTPROCESS_MM_TOKEN``
can supply secrets so they do not need to live in the TOML file.
"""
import abc
import io
import json
import os
import re
import smtplib
import time
import tomllib
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from loguru import logger


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class CommsConfig:
    """Communication configuration loaded from ``comms.toml``."""
    mode: str = "none"
    username: str = ""
    # Email
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_from: str = ""
    smtp_password: str = ""
    # Mattermost
    mm_server_url: str = ""
    mm_token: str = ""
    mm_channel_id: str = ""

    @classmethod
    def load(cls, path: str | Path | None = None) -> "CommsConfig":
        """Load comms configuration from the first TOML file found.

        Args:
            path: Explicit path to a comms.toml file.  When *None* the
                  standard search order described in the module docstring
                  is used.

        Returns:
            A populated CommsConfig (defaults to mode="none" if no file found).
        """
        candidates: list[Path] = []
        if path:
            candidates.append(Path(path))
        candidates.append(Path("comms.toml"))
        config_home = Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))
        candidates.append(config_home / "evn" / "comms.toml")
        try:
            candidates.append(Path(os.path.expanduser("~jops")) / ".config" / "evn" / "comms.toml")
        except RuntimeError:
            pass

        for p in candidates:
            if p.exists():
                with open(p, "rb") as f:
                    data = tomllib.load(f)
                logger.info(f"Loaded comms config from {p}")
                return cls._from_toml(data)

        logger.debug("No comms.toml found — notifications disabled.")
        return cls()

    @classmethod
    def _from_toml(cls, data: dict) -> "CommsConfig":
        """Build a ``CommsConfig`` from parsed TOML data.

        Args:
            data: Parsed TOML dictionary.

        Returns:
            Populated CommsConfig.
        """
        cfg = cls()
        cfg.mode = data.get("mode", "none").lower()
        cfg.username = data.get("username", "")

        email = data.get("email", {})
        cfg.smtp_host = email.get("smtp_host", "")
        cfg.smtp_port = email.get("smtp_port", 587)
        cfg.smtp_from = email.get("from_address", cfg.username)
        cfg.smtp_password = email.get("password", os.getenv("POSTPROCESS_SMTP_PASSWORD", ""))

        mm = data.get("mattermost", {})
        cfg.mm_server_url = mm.get("server_url", "").rstrip("/")
        cfg.mm_token = mm.get("token", os.getenv("POSTPROCESS_MM_TOKEN", ""))
        cfg.mm_channel_id = mm.get("channel_id", "")

        return cfg


# ---------------------------------------------------------------------------
# Notifier interface and implementations
# ---------------------------------------------------------------------------

class Notifier(abc.ABC):
    """Abstract base for notification back-ends."""

    @abc.abstractmethod
    def send_message(self, subject: str, body: str, attachments: list[Path] | None = None) -> bool:
        """Send a notification message.

        Args:
            subject: Short subject / title line.
            body: Message body (Markdown for Mattermost, plain text for email).
            attachments: Optional list of PNG file paths to attach.

        Returns:
            True on success, False on failure.
        """
        ...

    def wait_for_reply(self, timeout_seconds: int = 3600) -> str | None:
        """Wait for a reply from the user.

        Args:
            timeout_seconds: Maximum time to wait.

        Returns:
            Reply text, or None if unsupported / timed out.
        """
        return None

    def supports_interactive(self) -> bool:
        """Whether this back-end can receive replies from the user."""
        return False


class NoneNotifier(Notifier):
    """No-op notifier (mode ``none``)."""

    def send_message(self, subject: str, body: str, attachments: list[Path] | None = None) -> bool:
        """No-op: always returns True."""
        return True


class EmailNotifier(Notifier):
    """Send notifications via SMTP email."""

    def __init__(self, config: CommsConfig):
        self.config = config

    def send_message(self, subject: str, body: str, attachments: list[Path] | None = None) -> bool:
        """Build and send a MIME multipart email with optional PNG attachments.

        Args:
            subject: Email subject line.
            body: Plain-text message body.
            attachments: Optional list of PNG file paths to embed.

        Returns:
            True on success, False on failure.
        """
        try:
            msg = MIMEMultipart()
            msg["From"] = self.config.smtp_from
            msg["To"] = self.config.username
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            for filepath in (attachments or []):
                if filepath.exists() and filepath.suffix == ".png":
                    with open(filepath, "rb") as fh:
                        img = MIMEImage(fh.read(), name=filepath.name)
                    img.add_header("Content-Disposition", "attachment", filename=filepath.name)
                    msg.attach(img)

            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                server.starttls()
                if self.config.smtp_password:
                    server.login(self.config.smtp_from, self.config.smtp_password)
                server.send_message(msg)

            logger.info(f"Email sent to {self.config.username}: {subject}")
            return True
        except Exception as exc:
            logger.error(f"Failed to send email: {exc}")
            return False


class MattermostNotifier(Notifier):
    """Send notifications via the Mattermost REST API (personal access token).

    File uploads use ``multipart/form-data`` built with stdlib only so no
    extra dependencies are required.
    """

    def __init__(self, config: CommsConfig):
        self.config = config
        self._my_user_id: str | None = None
        self._channel_id: str = config.mm_channel_id
        self._last_post_ts: int = 0

    # -- low-level helpers --------------------------------------------------

    def _api(self, method: str, endpoint: str, data: dict | list | None = None) -> dict:
        """Make a JSON request to the Mattermost v4 API.

        Args:
            method: HTTP method (GET, POST, …).
            endpoint: API path starting with ``/`` (e.g. ``/users/me``).
            data: JSON-serialisable payload.

        Returns:
            Parsed JSON response.
        """
        url = f"{self.config.mm_server_url}/api/v4{endpoint}"
        headers = {"Authorization": f"Bearer {self.config.mm_token}", "Content-Type": "application/json"}
        body = json.dumps(data).encode() if data else None
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def _ensure_channel(self) -> None:
        """Create (or retrieve) a direct-message channel with the target user.

        Populates ``self._channel_id`` and ``self._my_user_id``.
        """
        if self._channel_id:
            if not self._my_user_id:
                me = self._api("GET", "/users/me")
                self._my_user_id = me["id"]
            return

        me = self._api("GET", "/users/me")
        self._my_user_id = me["id"]

        target = self._api("GET", f"/users/username/{self.config.username}")
        channel = self._api("POST", "/channels/direct", [self._my_user_id, target["id"]])
        self._channel_id = channel["id"]
        logger.info(f"Mattermost DM channel with @{self.config.username}: {self._channel_id}")

    def _upload_file(self, filepath: Path) -> str:
        """Upload a single file to the current channel.

        Args:
            filepath: Local path to the file.

        Returns:
            The Mattermost ``file_id``.
        """
        boundary = uuid.uuid4().hex
        url = f"{self.config.mm_server_url}/api/v4/files"

        buf = io.BytesIO()
        buf.write(f"--{boundary}\r\n".encode())
        buf.write(b'Content-Disposition: form-data; name="channel_id"\r\n\r\n')
        buf.write(f"{self._channel_id}\r\n".encode())
        buf.write(f"--{boundary}\r\n".encode())
        buf.write(f'Content-Disposition: form-data; name="files"; filename="{filepath.name}"\r\n'.encode())
        buf.write(b"Content-Type: image/png\r\n\r\n")
        buf.write(filepath.read_bytes())
        buf.write(f"\r\n--{boundary}--\r\n".encode())

        payload = buf.getvalue()
        headers = {
            "Authorization": f"Bearer {self.config.mm_token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        }
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read())

        return result["file_infos"][0]["id"]

    # -- public interface ---------------------------------------------------

    def send_message(self, subject: str, body: str, attachments: list[Path] | None = None) -> bool:
        """Post a Mattermost message (Markdown) with optional file uploads.

        Args:
            subject: Used as a Markdown heading in the message.
            body: Markdown body.
            attachments: Optional PNG files to upload alongside the post.

        Returns:
            True on success, False on failure.
        """
        try:
            self._ensure_channel()
            message = f"### {subject}\n\n{body}"

            file_ids: list[str] = []
            for path in (attachments or []):
                if path.exists() and path.suffix == ".png":
                    try:
                        file_ids.append(self._upload_file(path))
                    except Exception as exc:
                        logger.warning(f"Failed to upload {path.name}: {exc}")

            post_data: dict = {"channel_id": self._channel_id, "message": message}
            if file_ids:
                post_data["file_ids"] = file_ids

            result = self._api("POST", "/posts", post_data)
            self._last_post_ts = result.get("create_at", int(time.time() * 1000))
            logger.info(f"Mattermost message sent: {subject}")
            return True
        except Exception as exc:
            logger.error(f"Failed to send Mattermost message: {exc}")
            return False

    def supports_interactive(self) -> bool:
        """Mattermost supports receiving replies."""
        return True

    def wait_for_reply(self, timeout_seconds: int = 3600) -> str | None:
        """Poll the DM channel for a new message from the target user.

        Only messages created *after* the last ``send_message`` call are
        considered.  Messages posted by the bot itself are ignored.

        Args:
            timeout_seconds: Maximum seconds to wait before giving up.

        Returns:
            The reply text, or None on timeout / error.
        """
        if not self._channel_id or not self._last_post_ts:
            return None

        logger.info(f"Waiting for Mattermost reply (timeout: {timeout_seconds}s)…")
        deadline = time.time() + timeout_seconds
        seen: set[str] = set()

        while time.time() < deadline:
            try:
                posts = self._api("GET", f"/channels/{self._channel_id}/posts?since={self._last_post_ts}")
                for pid, post in posts.get("posts", {}).items():
                    if pid in seen:
                        continue
                    seen.add(pid)
                    if post.get("user_id") != self._my_user_id and post.get("message", "").strip():
                        logger.info(f"Received Mattermost reply: {post['message'][:100]}…")
                        return post["message"]
            except Exception as exc:
                logger.warning(f"Error polling Mattermost: {exc}")

            time.sleep(10)

        logger.info("Mattermost reply timeout reached.")
        return None


def make_notifier(config: CommsConfig) -> Notifier:
    """Factory: create the appropriate ``Notifier`` for the configured mode.

    Args:
        config: Loaded comms configuration.

    Returns:
        A concrete Notifier instance.
    """
    match config.mode:
        case "email":
            return EmailNotifier(config)
        case "mattermost":
            return MattermostNotifier(config)
        case _:
            return NoneNotifier()


# ---------------------------------------------------------------------------
# Message builders
# ---------------------------------------------------------------------------

def build_summary_text(exp) -> str:
    """Build a plain-text / Markdown experiment summary for notifications.

    Args:
        exp: ``experiment.Experiment`` object with metadata populated.

    Returns:
        Multi-line summary string.
    """
    from astropy import units as u

    lines: list[str] = [f"**Experiment:** {exp.expname}"]
    if exp.obsdate:
        line = f"**Obs. date:** {exp.obsdate.strftime('%d/%m/%Y')}"
        if exp.timerange:
            line += f" {exp.timerange[0].strftime('%H:%M')}-{exp.timerange[1].strftime('%H:%M')} UTC"
        lines.append(line)
    if exp.eEVNname:
        lines.append(f"**e-EVN run:** {exp.eEVNname}")
    for i, pi in enumerate(exp.pi):
        label = "P.I." if i == 0 else "co-PI"
        lines.append(f"**{label}:** {pi.name.capitalize()} ({pi.email})")
    lines.append(f"**Sup. Sci:** {exp.supsci.capitalize()}")
    if exp.refant:
        lines.append(f"**Ref. Ant:** {', '.join(exp.refant)}")
    if exp.feedback_page():
        lines.append(f"**Feedback:** {exp.feedback_page()}")
    if exp.archive_page:
        lines.append(f"**Archive:** {exp.archive_page}")

    # Sources
    lines.append("")
    for label, src_list in [("Fringe-finder", exp.sources.fringefinder),
                            ("Target", exp.sources.target), ("Phase-cal", exp.sources.calibrator)]:
        if src_list:
            lines.append(f"**{label}:** {', '.join(src_list)}")

    # Antennas
    observed = [a.name for a in exp.antennas if a.observed]
    missing = [a.name for a in exp.antennas if not a.observed]
    lines.append("")
    lines.append(f"**Antennas observed ({len(observed)}):** {', '.join(observed)}")
    if missing:
        lines.append(f"**Did not observe:** {', '.join(missing)}")
    if exp.antennas.polswap:
        lines.append(f"**Polswap:** {', '.join(exp.antennas.polswap)}")
    if exp.antennas.polconvert:
        lines.append(f"**PolConvert:** {', '.join(exp.antennas.polconvert)}")
    if exp.antennas.onebit:
        lines.append(f"**1-bit:** {', '.join(exp.antennas.onebit)}")

    # Correlator passes
    for i, cp in enumerate(exp.correlator_passes):
        if len(exp.correlator_passes) > 1:
            lines.append(f"\n**Pass #{i + 1}:**")
        if cp.freqsetup:
            lines.append(f"  Frequency: {cp.freqsetup.frequency.to(u.GHz):0.04}")
            lines.append(f"  Bandwidth: {cp.freqsetup.bandwidth.to(u.MHz):0.04} "
                         f"({int(cp.freqsetup.subbands)} subbands × {int(cp.freqsetup.channels)} ch)")
        fw = cp.flagged_weights
        if fw is not None:
            if fw.percentage is not None and fw.percentage >= 0:
                lines.append(f"  Weight flag: threshold {fw.threshold}, {fw.percentage:.2f}% flagged")
            else:
                lines.append(f"  Weight flag: threshold {fw.threshold} (not yet applied)")

    return "\n".join(lines)


def collect_plot_files(exp) -> list[Path]:
    """Collect PNG plot files from the experiment's plots directory.

    Args:
        exp: ``experiment.Experiment`` object.

    Returns:
        Sorted list of Path objects pointing to PNG plots.
    """
    if not exp.dirs or not exp.dirs.plots:
        return []
    return sorted(exp.dirs.plots.glob(f"{exp.expname.lower()}*.png"))


_MSOPS_REPLY_TEMPLATE = (
    "\n\n---\n"
    "**Please reply with the following information (one per line):**\n"
    "```\n"
    "weight_threshold: 0.85\n"
    "polswap: Wb, Jb\n"
    "onebit: none\n"
    "polconvert: Kt\n"
    "```\n"
    'Leave a field empty or write "none" if not applicable.\n'
    "Available antennas: {antennas}\n"
)


def parse_msops_reply(reply: str, exp) -> dict | None:
    """Parse a user's Mattermost reply into msops parameters.

    Expected format (one ``key: value`` per line)::

        weight_threshold: 0.85
        polswap: Wb, Jb
        onebit: none
        polconvert: Kt

    Args:
        reply: Raw reply text from the user.
        exp: Experiment object (used to validate antenna names).

    Returns:
        Dict with keys ``weight_threshold``, ``polswap``, ``onebit``,
        ``polconvert``; or *None* if parsing fails.
    """
    result: dict = {"weight_threshold": None, "polswap": [], "onebit": [], "polconvert": []}

    for line in reply.strip().splitlines():
        line = line.strip().strip("`")
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        key = key.strip().lower().replace(" ", "_")
        value = value.strip()

        if key == "weight_threshold":
            try:
                result["weight_threshold"] = float(value)
            except ValueError:
                logger.warning(f"Could not parse weight_threshold: {value}")
                return None
        elif key in ("polswap", "onebit", "polconvert"):
            if value.lower() in ("", "none", "-"):
                result[key] = []
            else:
                antennas = [a.strip().capitalize() for a in re.split(r"[,\s]+", value) if a.strip()]
                valid_names = set(exp.antennas.names)
                for ant in antennas:
                    if ant not in valid_names:
                        logger.warning(f"Unknown antenna '{ant}' in {key}")
                        return None
                result[key] = antennas

    if result["weight_threshold"] is None:
        logger.warning("Missing weight_threshold in reply")
        return None

    return result


def apply_msops_feedback(exp, feedback: dict) -> bool:
    """Apply parsed msops feedback to the experiment object.

    Performs the same mutations as ``Terminal.askMSoperations()`` or
    ``PolicyDriven.askMSoperations()``: sets the weight-flag threshold
    on every correlator pass and toggles per-antenna polswap / polconvert /
    onebit flags.

    Args:
        exp: ``experiment.Experiment`` to mutate.
        feedback: Dict returned by :func:`parse_msops_reply`.

    Returns:
        True on success.
    """
    from . import experiment as _experiment

    threshold = feedback["weight_threshold"]
    for i in range(len(exp.correlator_passes)):
        existing = exp.correlator_passes[i].flagged_weights
        if existing and existing.threshold == threshold and existing.percentage >= 0:
            logger.info(f"flag_weights threshold unchanged ({threshold}) for "
                        f"{exp.correlator_passes[i].msfile.name}")
        else:
            exp.correlator_passes[i].flagged_weights = _experiment.FlagWeight(threshold, -1)

    for antenna in feedback.get("polswap", []):
        exp.antennas[antenna].polswap = True
    for antenna in feedback.get("polconvert", []):
        exp.antennas[antenna].polconvert = True
    for antenna in feedback.get("onebit", []):
        exp.antennas[antenna].onebit = True

    return True


# ---------------------------------------------------------------------------
# High-level notification helpers (called from workflow.py)
# ---------------------------------------------------------------------------

def notify_dashboard_review(exp, notifier: Notifier) -> dict | None:
    """Send dashboard-review notification with experiment summary and plots.

    For a ``MattermostNotifier`` this also waits for interactive feedback from
    the user and returns the parsed msops parameters.  For ``EmailNotifier``
    (or ``NoneNotifier``) it sends a message and returns *None* so the caller
    falls through to the regular dialog.

    Args:
        exp: ``experiment.Experiment`` object.
        notifier: Concrete Notifier instance.

    Returns:
        Parsed feedback dict (from :func:`parse_msops_reply`) when
        interactive feedback was received, or *None*.
    """
    if isinstance(notifier, NoneNotifier):
        return None

    summary = build_summary_text(exp)
    plots = collect_plot_files(exp)

    subject = f"EVN Post-Processing: {exp.expname} — Dashboard Review"

    body = summary
    if notifier.supports_interactive():
        body += _MSOPS_REPLY_TEMPLATE.format(antennas=", ".join(exp.antennas.names))
    else:
        body += ("\n\n---\n"
                 "Please log in to the server to review the standard plots "
                 "and continue the post-processing.")

    if not notifier.send_message(subject, body, plots):
        return None

    if notifier.supports_interactive():
        reply = notifier.wait_for_reply(timeout_seconds=86400)  # 24 h
        if reply:
            feedback = parse_msops_reply(reply, exp)
            if feedback:
                confirm_body = (
                    f"Received feedback:\n"
                    f"- **weight_threshold:** {feedback['weight_threshold']}\n"
                    f"- **polswap:** {', '.join(feedback['polswap']) or 'none'}\n"
                    f"- **onebit:** {', '.join(feedback['onebit']) or 'none'}\n"
                    f"- **polconvert:** {', '.join(feedback['polconvert']) or 'none'}\n\n"
                    f"Applying and continuing post-processing…"
                )
                notifier.send_message(f"{exp.expname} — Feedback Applied", confirm_body)
                return feedback

            notifier.send_message(
                f"{exp.expname} — Parse Error",
                "Could not parse your reply. Please log in to the server to continue manually.",
            )

    return None


def notify_step_pause(exp, step: str, reason: str, notifier: Notifier) -> None:
    """Send an informational notification that the workflow has paused.

    No interactive reply is expected — the user must log in anyway for
    further verification.

    Args:
        exp: ``experiment.Experiment`` object.
        step: Name of the workflow step that just completed.
        reason: Human-readable explanation of the pause.
        notifier: Concrete Notifier instance.
    """
    if isinstance(notifier, NoneNotifier):
        return

    summary = build_summary_text(exp)
    subject = f"EVN Post-Processing: {exp.expname} — Paused after '{step}'"
    body = (
        f"{summary}\n\n---\n\n"
        f"The post-processing has paused after step **{step}**.\n\n"
        f"**Reason:** {reason}\n\n"
        f"Please review the pipeline output and PI letter, then run:\n"
        f"  `postprocess run` to continue\n"
        f"  `postprocess run {step}` to re-run this step"
    )
    notifier.send_message(subject, body)
