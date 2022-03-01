
## Unit testing

launch a clamav server on port 3310

```shell
docker run --rm -d \
    --name clamav \
    -p 3310:3310 \
    public.ecr.aws/equancy-tech/datalake-catalog:1.0.0
```

for GCP, get credentials for **equancyrandd**

for AWS, get a service account for **equancy-lab**

for Azure, get a service principal for **Azure Equancy**

```shell
export AWS_PROFILE='equancy-lab'
export GOOGLE_APPLICATION_CREDENTIALS='path/to/key.json'
export AZURE_TENANT_ID='4a134cf6-5468-45e4-859b-bd3cc08223a2'
export AZURE_CLIENT_ID='********-****-****-****-************'
export AZURE_CLIENT_SECRET='*************************************'

poetry install
poetry run coverage run -m pytest && poetry run coverage report -m
```