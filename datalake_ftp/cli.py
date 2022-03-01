from datalake_ftp import FTPCloud
import click


@click.command()
@click.option("-d", "--daemon", is_flag=True)
@click.option("-c", "--config", required=True, type=click.File())
def main(daemon, config):
    cfg = yaml.safe_load(config)
    ftp_cloud = FTPCloud(cfg)
