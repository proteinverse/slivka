import pytest
import yaml

from slivka.compat import resources
from slivka.db.documents import JobRequest
from slivka.db.helpers import insert_many
from slivka.db.repositories import RequestsRepository


@pytest.fixture()
def requests_repository(database):
    return RequestsRepository(database)


@pytest.fixture()
def job_requests(request, database):
    stream = resources.open_text(__package__, request.param)
    data = [
        JobRequest(**kwargs) for kwargs in yaml.load(stream, yaml.SafeLoader)
    ]
    insert_many(database, data)
    return data


@pytest.mark.parametrize(
    "job_requests",
    ["testdata/requests_set_2.yaml"],
    indirect=True
)
@pytest.mark.parametrize(
    "filters, expected_output",
    [
        ([], 6),
        ([("service", "example-1")], 3),
        ([("submissionTime", "2020-05-22")], 2),
        ([("submissionTime", "<=2020-05-22"), ("service", "example-1")], 1)
    ]
)
def test_count_filtered(requests_repository, job_requests, filters, expected_output):
    requests_repository.count()