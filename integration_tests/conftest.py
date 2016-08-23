import subprocess
import time
import pytest
from simcity import FileConfig, RestRequests


@pytest.fixture(scope="session", autouse=True)
def docker_compose(request):
    """ Run docker-compose """
    print("building docker images and starting containers")
    compose = subprocess.Popen(['docker-compose', 'up', '--build', '-d'],
                               cwd='integration_tests/docker')
    compose.wait()
    assert 0 == compose.returncode
    time.sleep(10)

    yield

    if request.node.session.testsfailed > 0:
        print("docker logs")
        subprocess.Popen(['docker-compose', 'logs'],
                         cwd='integration_tests/docker').wait()

    print("Stopping docker containers")
    subprocess.Popen(['docker-compose', 'stop'],
                     cwd='integration_tests/docker').wait()

    subprocess.Popen(['docker-compose', 'rm', '-f'],
                     cwd='integration_tests/docker').wait()

    assert request.node.session.testsfailed == 0


@pytest.fixture
def dav():
    config = FileConfig('integration_tests/docker/config.ini')
    dav_cfg = config.section('webdav')

    if 'username' in dav_cfg:
        auth = (dav_cfg['username'], dav_cfg['password'])
    else:
        auth = None

    return RestRequests(dav_cfg['url'], auth=auth)
