from glob import glob
from logging import getLogger
from pathlib import Path
from os import makedirs
from os.path import join
import pendulum
import shutil
from subprocess import run
from datalake import ServiceDiscovery
from datalake.telemetry import Measurement

STATUS_SUCCESS = "Success"
STATUS_ERROR = "Failure"
STATUS_INFECTED = "Infected"


class FTPCloud:
    def __init__(self, config={}):
        self._logger = getLogger(f"{__name__}.{__class__.__name__}")

        if not isinstance(config, dict):
            raise ValueError("FTPCloud configuration must be a dict")

        self._config = {
            "cloud": {
                "bucket": ".",
                "prefix": None,
                "provider": "local",
                "monitoring": {
                    "class": "NoMonitor",
                    "params": {},
                },
            },
            "ftp_dir": "/ftp/home",
            "drop_folder": "INPUT",
            "deliver_folder": "LANDING",
            "archive_folder": "ARCHIVE",
            "quarantine_folder": "QUARANTINE",
            "move_age_seconds": 180,
            "archive_retention_hours": 24,
            "antivirus": {
                "enabled": False,
            },
        }
        self._config.update(config)

        self._ftp_dir = Path(self._config["ftp_dir"])
        self._services = ServiceDiscovery(**self._config["cloud"])

    @property
    def ftp_dir(self):
        return self._ftp_dir

    def full_path(self, path):
        return self._ftp_dir / path

    def scan_folder(self, folder, min_age=60):
        """
        List files in a folder across FTP users.

        :param subfolder: the subfolder in FTP users' home to scan.
        :param min_age: the mtime threshold in seconds (default is 60).
        :return: The ``list`` of ``Path`` which mtime is older that ``min_age`` seconds.
        """
        rtime = pendulum.now()
        scan_files = self._ftp_dir.glob(f"*/{folder}/**/*")
        scan_result = []
        for scan_file in scan_files:
            if scan_file.is_file():
                relative_file = scan_file.relative_to(self._ftp_dir)
                mtime = pendulum.from_timestamp(scan_file.stat().st_mtime)
                etime = mtime.diff(rtime).in_seconds()
                if etime > min_age:
                    scan_result.append(relative_file)
        return scan_result

    def move_to(self, src_path, folder):
        """
        Move a file from a user subfolder to another subfolder (yet for the same user)

        :param src_path: the ``Path`` of the file to move.
        :param folder: the subfolder to move the file to.
        :return: ``None``
        """
        target_parts = list(src_path.parts)
        target_parts[1] = folder
        target_path = self.full_path(Path(*target_parts))
        target_parent = target_path.parent
        makedirs(str(target_parent), mode=0o755, exist_ok=True)
        source_path = self.full_path(src_path)
        shutil.move(str(source_path), str(target_path))

    def delta3(self):
        files_to_move = self.scan_folder(folder=self._config["drop_folder"], min_age=self._config["move_age_seconds"])
        for file_to_move in files_to_move:
            metric = Measurement("ftpcloud-received")
            metric.add_labels({"file_path": str(file_to_move)})
            metric.add_measure("file_size", self.full_path(file_to_move).stat().st_size)
            try:
                self.move_to(file_to_move, self._config["deliver_folder"])
                metric.add_label("status", STATUS_SUCCESS)
            except Exception as e:
                self._logger.error(f"An error occured whilst moving {files_to_move}: {str(e)}")
                metric.add_label("status", STATUS_ERROR)

            self._services.monitor.push(metric)

    def lambda1(self):
        files_to_move = self.scan_folder(folder=self._config["deliver_folder"], min_age=0)
        for file_to_move in files_to_move:
            metric = Measurement("ftpcloud-delivered")
            metric.add_labels({"file_path": str(file_to_move)})
            metric.add_measure("file_size", self.full_path(file_to_move).stat().st_size)

            try:
                is_safe = True
                if self._config["antivirus"]["enabled"]:
                    clamav = run(
                        f"clamdscan --no-summary --fdpass {file_to_scan}",
                        shell=True,
                        text=True,
                        capture_output=True,
                    )
                    status = clamav.stdout.replace(f"{file_to_scan}: ", "")
                    if clamav.returncode == 0:
                        is_safe = True
                    elif clamav.returncode == 1:
                        metric.add_label("virus_name", status)
                        metric.add_label("status", STATUS_INFECTED)
                        self._logger.warn(f"Virus detected in file {files_to_move}: {status}")
                        is_safe = False
                    else:
                        raise IOError(clamav.stderr)

                if is_safe:
                    bucket = self._services.get_storage(self._config["cloud"]["bucket"])
                    if self._config["cloud"]["prefix"] is None:
                        target_path = str(files_to_move)
                    else:
                        target_path = join(self._config["cloud"]["prefix"], str(file_to_move))
                    bucket.upload(
                        src=str(self.full_path(file_to_move)),
                        dst=target_path,
                        content_type="octet/stream",
                    )
                    self.move_to(file_to_move, self._config["archive_folder"])
                    metric.add_label("status", STATUS_SUCCESS)
                else:
                    self.move_to(file_to_move, self._config["quarantine_folder"])
            except Exception as e:
                self._logger.error(f"An error occured whilst delivering {files_to_move}: {str(e)}")
                metric.add_label("status", STATUS_ERROR)

            self._services.monitor.push(metric)
