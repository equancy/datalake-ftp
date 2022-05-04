
## Unit testing

```shell
export AWS_PROFILE='equancy-lab'

poetry install
poetry run coverage run -m pytest && poetry run coverage report -m
```

> If ClamAV cannot be launched, tests can be run without antivirus scanning by adding the following marker to pytest `-m "not clamav"`
