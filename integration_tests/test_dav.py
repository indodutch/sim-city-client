from simcity import FileConfig, RestRequests
from nose.tools import assert_equals, assert_raises, assert_true, assert_false
from tempfile import TemporaryFile, TemporaryDirectory
import os


def get_dav():
    config = FileConfig('integration_tests/docker/config.ini')
    dav_cfg = config.section('webdav')

    if 'username' in dav_cfg:
        auth = (dav_cfg['username'], dav_cfg['password'])
    else:
        auth = None

    return RestRequests(dav_cfg['url'], auth=auth)


def test_put_bytes():
    dav = get_dav()
    dav.put('something.txt', b'')
    assert_equals(b'', dav.get('something.txt'))
    dav.put('something.txt', b'aaa')
    assert_equals(b'aaa', dav.get('something.txt'))


def test_put_file():
    dav = get_dav()
    with TemporaryFile() as f:
        assert_raises(ValueError, dav.put, 'something.txt', f)
        dav.put('something.txt', f, content_length=0)
        assert_equals(b'', dav.get('something.txt'))
        f.write(b'aaa')
        f.seek(0, 0)
        dav.put('something.txt', f, content_length=3)
        assert_equals(b'aaa', dav.get('something.txt'))

    with TemporaryDirectory() as d:
        file_path = os.path.join(d, 'something.txt')
        dav.download('something.txt', file_path)
        with open(file_path, 'rb') as f:
            assert_equals(b'aaa', f.read())


def test_delete_exists():
    dav = get_dav()
    assert_true(dav.exists('something.txt'))
    dav.delete('something.txt')
    assert_false(dav.exists('something.txt'))
    assert_raises(IOError, dav.delete, 'something.txt')
    dav.delete('something.txt', ignore_not_existing=True)


def test_mkdir_delete():
    dav = get_dav()
    assert_false(dav.exists('somedir'))
    assert_raises(IOError, dav.put, 'somedir/sometext.txt', b'aaa')
    dav.mkdir('somedir')
    assert_true(dav.exists('somedir'))
    dav.put('somedir/sometext.txt', b'aaa')
    assert_true(dav.exists('somedir/sometext.txt'))
    assert_raises(IOError, dav.mkdir, 'somedir')
    dav.mkdir('somedir', ignore_existing=True)
    dav.delete('somedir')
    assert_false(dav.exists('somedir'))
    assert_false(dav.exists('somedir/sometext.txt'))
