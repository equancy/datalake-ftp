from os import utime
from os.path import join
import pytest
from datalake_ftp import FTPCloud
from pathlib import Path
from shutil import copytree, rmtree
from tempfile import mkdtemp
from pendulum import now
from uuid import uuid4
from datalake.provider.aws import Storage

S3_BUCKET = "eqlab-datamock-ephemeral"


@pytest.fixture(scope="module")
def test_id():
    return uuid4()


@pytest.fixture(scope="module")
def temp_dir():
    temp_dir = mkdtemp(prefix="datalake-ftp", suffix="_home")
    yield temp_dir
    rmtree(temp_dir)


@pytest.fixture(scope="module")
def ftp_dir(temp_dir):
    copytree("./tests/ftp", temp_dir, dirs_exist_ok=True)

    d30 = now().subtract(seconds=30).int_timestamp
    d80 = now().subtract(seconds=80).int_timestamp
    d600 = now().subtract(seconds=600).int_timestamp
    utime(join(temp_dir, "bob/INPUT/data-1.csv"), (d600, d600))
    utime(join(temp_dir, "bob/INPUT/data-2.csv"), (d80, d80))
    utime(join(temp_dir, "bob/INPUT/data-3.csv"), (d30, d30))
    utime(join(temp_dir, "alice/INPUT/global.xml"), (d600, d600))
    utime(join(temp_dir, "alice/INPUT/domain/2049_domain.xml"), (d600, d600))
    utime(join(temp_dir, "alice/INPUT/domain/feed/2049_feed.xml"), (d600, d600))
    utime(join(temp_dir, "alice/INPUT/eicar.com.txt"), (now().int_timestamp, now().int_timestamp))

    return temp_dir


@pytest.fixture(scope="module")
def ftp_cloud(ftp_dir, test_id):
    config = {
        "cloud": {
            "bucket": S3_BUCKET,
            "prefix": f"ftp-{test_id}",
            "provider": "aws",
            "monitoring": {
                "class": "NoMonitor",
                "params": {
                    "quiet": False,
                },
            },
        },
        "ftp_dir": ftp_dir,
    }

    return FTPCloud(config)


@pytest.fixture(scope="module")
def ftp_cloudav(ftp_dir, test_id):
    config = {
        "cloud": {
            "bucket": S3_BUCKET,
            "prefix": f"ftp-{test_id}",
            "provider": "aws",
            "monitoring": {
                "class": "NoMonitor",
                "params": {
                    "quiet": False,
                },
            },
        },
        "ftp_dir": ftp_dir,
        "antivirus": {
            "enabled": True,
            "params": "--config-file tests/clamd.conf --stream"
        },
    }

    return FTPCloud(config)


def test_scan_files(ftp_cloud):
    l = [str(i) for i in ftp_cloud.scan_folder("INPUT", 120)]
    assert len(l) == 4
    assert "alice/INPUT/global.xml" in l
    assert "bob/INPUT/data-1.csv" in l
    assert "bob/INPUT/data-2.csv" not in l
    assert "bob/INPUT/data-3.csv" not in l

    l = [str(i) for i in ftp_cloud.scan_folder("INPUT", 60)]
    assert len(l) == 5
    assert "alice/INPUT/global.xml" in l
    assert "bob/INPUT/data-1.csv" in l
    assert "bob/INPUT/data-2.csv" in l
    assert "bob/INPUT/data-3.csv" not in l

    l = [str(i) for i in ftp_cloud.scan_folder("INPUT", 20)]
    assert len(l) == 6
    assert "alice/INPUT/global.xml" in l
    assert "bob/INPUT/data-1.csv" in l
    assert "bob/INPUT/data-2.csv" in l
    assert "bob/INPUT/data-3.csv" in l


def test_prepare(ftp_cloud):
    ftp_dir = Path(ftp_cloud.ftp_dir)
    assert (ftp_dir / Path("alice/INPUT/global.xml")).is_file()
    assert not (ftp_dir / Path("alice/LANDING")).is_dir()
    assert not (ftp_dir / Path("alice/LANDING/global.xml")).is_file()
    ftp_cloud.move_to(Path("alice/INPUT/global.xml"), "LANDING")
    assert not (ftp_dir / Path("alice/INPUT/global.xml")).is_file()
    assert (ftp_dir / Path("alice/LANDING/global.xml")).is_file()
    assert (ftp_dir / Path("alice/INPUT/domain/feed/2049_feed.xml")).is_file()

    assert not (ftp_dir / Path("alice/LANDING/domain")).is_dir()
    assert not (ftp_dir / Path("alice/LANDING/domain/feed")).is_dir()
    assert not (ftp_dir / Path("alice/LANDING/domain/feed/2049_feed.xml")).is_file()
    ftp_cloud.move_to(Path("alice/INPUT/domain/feed/2049_feed.xml"), "LANDING")
    assert not (ftp_dir / Path("alice/INPUT/domain/feed/2049_feed.xml")).is_file()
    assert (ftp_dir / Path("alice/LANDING/domain/feed")).is_dir()
    assert (ftp_dir / Path("alice/LANDING/domain/feed/2049_feed.xml")).is_file()


def test_delta3(ftp_cloud):
    ftp_dir = Path(ftp_cloud.ftp_dir)
    assert (ftp_dir / Path("bob/INPUT/data-1.csv")).is_file()
    assert (ftp_dir / Path("alice/INPUT/domain/2049_domain.xml")).is_file()
    assert not (ftp_dir / Path("bob/LANDING/data-1.csv")).is_file()
    assert not (ftp_dir / Path("alice/LANDING/domain/2049_domain.xml")).is_file()
    ftp_cloud.delta3()
    assert not (ftp_dir / Path("bob/INPUT/data-1.csv")).is_file()
    assert not (ftp_dir / Path("alice/INPUT/domain/2049_domain.xml")).is_file()
    assert (ftp_dir / Path("bob/LANDING/data-1.csv")).is_file()
    assert (ftp_dir / Path("alice/LANDING/domain/2049_domain.xml")).is_file()


def test_lambda1(ftp_cloud, test_id):
    ftp_dir = Path(ftp_cloud.ftp_dir)
    cloud = Storage(S3_BUCKET)
    assert (ftp_dir / Path("bob/LANDING/data-1.csv")).is_file()
    assert (ftp_dir / Path("alice/LANDING/domain/2049_domain.xml")).is_file()
    assert not (ftp_dir / Path("bob/ARCHIVE/data-1.csv")).is_file()
    assert not (ftp_dir / Path("alice/ARCHIVE/domain/2049_domain.xml")).is_file()
    assert not cloud.exists(f"ftp-{test_id}/bob/LANDING/data-1.csv")
    assert not cloud.exists(f"ftp-{test_id}/alice/ARCHIVE/domain/2049_domain.xml")
    ftp_cloud.lambda1()
    assert not (ftp_dir / Path("bob/LANDING/data-1.csv")).is_file()
    assert not (ftp_dir / Path("alice/LANDING/domain/2049_domain.xml")).is_file()
    assert (ftp_dir / Path("bob/ARCHIVE/data-1.csv")).is_file()
    assert (ftp_dir / Path("alice/ARCHIVE/domain/2049_domain.xml")).is_file()
    assert cloud.exists(f"ftp-{test_id}/bob/LANDING/data-1.csv")
    assert cloud.exists(f"ftp-{test_id}/alice/LANDING/domain/2049_domain.xml")


@pytest.mark.clamav
def test_antivirus(ftp_cloudav, test_id, caplog):
    ftp_dir = Path(ftp_cloudav.ftp_dir)
    ftp_cloudav.move_to(Path("alice/INPUT/eicar.com.txt"), "LANDING")
    cloud = Storage(S3_BUCKET)
    assert (ftp_dir / Path("alice/LANDING/eicar.com.txt")).is_file()
    assert not (ftp_dir / Path("alice/QUARANTINE/eicar.com.txt")).is_file()
    assert not cloud.exists(f"ftp-{test_id}/alice/LANDING/eicar.com.txt")
    ftp_cloudav.lambda1()
    assert not (ftp_dir / Path("alice/LANDING/eicar.com.txt")).is_file()
    assert (ftp_dir / Path("alice/QUARANTINE/eicar.com.txt")).is_file()
    assert not cloud.exists(f"ftp-{test_id}/alice/LANDING/eicar.com.txt")
    assert caplog.records[0].levelname == "WARNING"
    assert "EICAR" in caplog.text
