
## Unit testing

launch a clamav server on port 3310

```shell
docker run --rm -d \
    --name clamav \
    -p 3310:3310 \
    public.ecr.aws/equancy-tech/datalake-catalog:1.0.0
```

```shell
export AWS_PROFILE='equancy-lab'

poetry install
poetry run coverage run -m pytest && poetry run coverage report -m
```

> If ClamAV cannot be launched, tests can be run without antivirus scanning by adding the following marker to pytest `-m "not clamav"`
