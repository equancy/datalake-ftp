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
from copy import deepcopy

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

    d10 = now().subtract(seconds=10).int_timestamp
    d30 = now().subtract(seconds=30).int_timestamp
    d80 = now().subtract(seconds=80).int_timestamp
    d600 = now().subtract(seconds=600).int_timestamp
    h80 = now().subtract(hours=80).int_timestamp
    utime(join(temp_dir, "bob/INPUT/data-1.csv"), (d600, d600))
    utime(join(temp_dir, "bob/INPUT/data-2.csv"), (d80, d80))
    utime(join(temp_dir, "bob/INPUT/data-3.csv"), (d30, d30))
    utime(join(temp_dir, "alice/INPUT/global.xml"), (d600, d600))
    utime(join(temp_dir, "alice/INPUT/domain/2049_domain.xml"), (d600, d600))
    utime(join(temp_dir, "alice/INPUT/domain/feed/2049_feed.xml"), (d600, d600))
    utime(join(temp_dir, "alice/INPUT/eicar.com.txt"), (d10, d10))
    utime(join(temp_dir, "alice/INPUT/lorem.txt"), (d10, d10))
    utime(join(temp_dir, "jose/ARCHIVE/oldie.txt"), (h80, h80))

    return temp_dir


@pytest.fixture(scope="module")
def base_config(ftp_dir, test_id):
    return {
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


@pytest.fixture(scope="module")
def ftp_cloud(base_config):
    return FTPCloud(base_config)


@pytest.fixture(scope="module")
def ftp_cloudav(base_config):
    config = deepcopy(base_config)
    config["antivirus"] = {"enabled": True, "params": "--config-file tests/clamd.conf --stream"}
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


def test_template(base_config):
    input_path1 = Path("alice/LANDING/domain/feed/2049_feed.xml")
    input_path2 = Path("bob/LANDING/data-3.csv")
    today = now().to_date_string()
    config = deepcopy(base_config)

    # By default the target path matches the input path
    ftp_cloud = FTPCloud(config)
    assert ftp_cloud.target_path(input_path1) == str(input_path1)

    config["target_template"] = "{user}/{folder}/{date}/{filename}"
    ftp_cloud = FTPCloud(config)
    assert ftp_cloud.target_path(input_path1) == f"alice/domain/feed/{today}/2049_feed.xml"
    # Empty subfolders are removed
    assert ftp_cloud.target_path(input_path2) == f"bob/{today}/data-3.csv"
    # Absolute template is ignored
    config["target_template"] = "/landing/{folder}/{user}/{filename}"
    ftp_cloud = FTPCloud(config)
    assert ftp_cloud.target_path(input_path2) == f"landing/bob/data-3.csv"
    
    config["target_template"] = "static_file.txt"
    ftp_cloud = FTPCloud(config)
    assert ftp_cloud.target_path(input_path1) == "static_file.txt"


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
    assert not cloud.exists(f"ftp-{test_id}/alice/LANDING/domain/2049_domain.xml")
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
    ftp_cloudav.move_to(Path("alice/INPUT/lorem.txt"), "LANDING")
    cloud = Storage(S3_BUCKET)
    assert (ftp_dir / Path("alice/LANDING/eicar.com.txt")).is_file()
    assert (ftp_dir / Path("alice/LANDING/lorem.txt")).is_file()
    assert not (ftp_dir / Path("alice/QUARANTINE/eicar.com.txt")).is_file()
    assert not (ftp_dir / Path("alice/QUARANTINE/lorem.txt")).is_file()
    assert not cloud.exists(f"ftp-{test_id}/alice/LANDING/eicar.com.txt")
    assert not cloud.exists(f"ftp-{test_id}/alice/LANDING/lorem.txt")
    ftp_cloudav.lambda1()
    assert not (ftp_dir / Path("alice/LANDING/eicar.com.txt")).is_file()
    assert not (ftp_dir / Path("alice/LANDING/lorem.txt")).is_file()
    assert (ftp_dir / Path("alice/QUARANTINE/eicar.com.txt")).is_file()
    assert not (ftp_dir / Path("alice/QUARANTINE/lorem.txt")).is_file()
    assert not cloud.exists(f"ftp-{test_id}/alice/LANDING/eicar.com.txt")
    assert cloud.exists(f"ftp-{test_id}/alice/LANDING/lorem.txt")
    assert caplog.records[0].levelname == "WARNING"
    assert "EICAR" in caplog.text


def test_delta24(ftp_cloud):
    ftp_dir = Path(ftp_cloud.ftp_dir)
    assert (ftp_dir / Path("jose/ARCHIVE/oldie.txt")).is_file()
    assert (ftp_dir / Path("bob/ARCHIVE/data-1.csv")).is_file()
    assert (ftp_dir / Path("alice/ARCHIVE/domain/2049_domain.xml")).is_file()
    ftp_cloud.delta24()
    assert not (ftp_dir / Path("jose/ARCHIVE/oldie.txt")).is_file()
    assert (ftp_dir / Path("bob/ARCHIVE/data-1.csv")).is_file()
    assert (ftp_dir / Path("alice/ARCHIVE/domain/2049_domain.xml")).is_file()
