from datalake_ftp import FTPCloud
import click
from time import sleep
from yaml import safe_load


@click.command()
@click.option("-d", "--daemon", is_flag=True)
@click.option("-c", "--config", required=True, type=click.File())
def main(daemon, config):
    cfg = safe_load(config)
    ftp_cloud = FTPCloud(cfg)
    if daemon:
        while True:
            ftp_cloud.delta3()
            ftp_cloud.lambda1()
            ftp_cloud.delta24()
            sleep(60)
    else:
        ftp_cloud.delta3()
        ftp_cloud.lambda1()
        ftp_cloud.delta24()
