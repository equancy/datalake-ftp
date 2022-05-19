# Datalake FTP

Moves files from FTP drop folders to a Cloud Bucket.

## How it works

In the following FTP folder hierarchy, there are two users **bob** and **alice**. 
Each users have four subfolders `INPUT`, `LANDING`, `ARCHIVE` and `QUARANTINE`.

```plaintext
/ftp/home/
|-- bob/
|   |-- INPUT/
|   |-- LANDING/
|   |-- ARCHIVE/
|   `-- QUARANTINE/
`-- alice/
    |-- INPUT/
    |-- LANDING/
    |-- ARCHIVE/
    `-- QUARANTINE/
```

The goal is to synchronize all files the FTP users put in the `INPUT` folder with a cloud bucket ([AWS S3](https://aws.amazon.com/s3/), [Google Cloud Storage](https://cloud.google.com/storage/) or [Azure BlobStorage](https://azure.microsoft.com/en-us/services/storage/blobs/)).
To ensure a file is completely uploaded, its modification time must be _old enough_, like 3 minutes.
The synchronization process is split in the following steps:

1. Select files ready for synchronization: a file is selected if it is in the `INPUT` folder (or one of its subfolders) and if it is older than 3 minutes (according to the last modification time). 
Selected files are moved to the `LANDING` folder.

2. **Optional** Scan files for malwares and viruses: all files in the `LANDING` folder are scanned with [ClamAV antivirus](https://www.clamav.net/).
Infected files are moved to the `QUARANTINE` folder.

> :warning: ClamAV antivirus only works on Linux.

3. Copy files to the cloud bucket: all files in the `LANDING` folder are copied along with their subfolders in a target bucket. Files successfully copied are then moved to the `ARCHIVE` folder.

4. Clean up files after retention period: all files older than 72 hours (according to last modification time) are removed from the folders `ARCHIVE` and `QUARANTINE`


## Folder permissions

`ftpcloud` needs permissions to scan `INPUT` folders across all ftp users and needs permissions to move files from one subfolder to another.

Here is a working example of user/group permission setup

First, create a user for `ftpcloud` command

```shell
sudo useradd -s /bin/bash -g sftp -m -N -c "Datalake Transfer Agent" datalake
```

Then, create a FTP user folders with the following permissions

```shell
sudo mkdir -m 0775 INPUT/
sudo chown ${MY_USER}:sftp INPUT/

sudo mkdir -m 0755 LANDING/ ARCHIVE/ QUARANTINE/
sudo chown datalake:sftp LANDING/ ARCHIVE/ QUARANTINE/
```

## Installation

Install the package using pip

```shell
pip install datalake-ftp
```

The following command executes a synchronization with a provided configuration file

```shell
ftpcloud -c 'path/to/config.yaml'
```

The command can be configured as a Linux service. 
For example using systemd:

```ini
[Unit]
Description=Datalake File Transfer
Requires=network.target
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/ftpcloud --daemon --config /etc/datalake-ftp/config.yml
Restart=always
User=datalake
Group=sftp

[Install]
WantedBy=multi-user.target
```


## Configuration

Configuration file is in YAML format.

Here is a sample configuration with **Azure BlobStorage**

```yaml
cloud: 
  bucket: mystorageaccount.mycontainer
  prefix: external/file-transfer
  provider: azure
  monitoring: 
    class: NoMonitor
    params:
      quiet: no
ftp_dir: /data/ftp/home
drop_folder: INPUT
deliver_folder: EXPORTING
archive_folder: EXPORTED
quarantine_folder: QUARANTINED
target_template: "FTP-{user}/{folder}/{date}_{filename}"
```

Another example using **GCS bucket** with an active antivirus

```yaml
cloud: 
  bucket: my-bcs-bucket
  prefix: ""
  provider: gcp
  monitoring: 
    class: datalake.provider.gcp.GoogleMonitor
    params:
      project_id: my-project-id
antivirus:
  enabled: yes
  params: "--fdpass"
```


The following options are available:


| Option | Description | Default value |
| ------ | ----------- | ------------- |
| `ftp_dir` | The parent folder for all FTP users | "/ftp/home" |
| `drop_folder` | The name for the `INPUT` folder | "INPUT" |
| `deliver_folder` | The name for the `LANDING` folder | "LANDING" |
| `archive_folder` | The name for the `ARCHIVE` folder | "ARCHIVE" |
| `quarantine_folder` | The name for the `QUARANTINE` folder | "QUARANTINE" |
| `move_age_seconds` | The age threshold in seconds for selecting files for synchronization | 180 |
| `archive_retention_hours` | The retention period in hours for archived and quarantined files | 72 |
| `target_template` | The template string for generating the target path in the bucket | "{fullpath}" |
| `cloud.bucket` | The name of the target bucket | "." |
| `cloud.prefix` | The prefix to append to file names before copying to the bucket. | "" |
| `cloud.provider` | The cloud provider for the bucket (either "aws", "gcp", "azure" or "local")| "local" |
| `cloud.monitoring` | The monitoring configuration | `NoMonitor` |
| `antivirus.enabled` | The flag to enable antivirus scanning | `false` |
| `antivirus.params` | Optional arguments to pass to `clamdscan` command | "" |

The placeholders for templating `target_template` are

- **fullpath**: the exact input path
- **user**: the username part from the input path
- **folder**: the subfolders relative to `deliver_folder` from the input path
- **filename**: the filename part from the input path
- **date**: the current day in ISO-8601 format
