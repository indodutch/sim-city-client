import pytest


def test_put_bytes(dav):
    dav.put('something.txt', b'')
    assert b'' == dav.get('something.txt')
    dav.put('something.txt', b'aaa')
    assert b'aaa' == dav.get('something.txt')


def test_put_file(dav, tmpdir):
    f = tmpdir.join('something.txt')
    pytest.raises(ValueError, dav.put, 'something.txt', f.open('wb'))
    dav.put('something.txt', f.open('wb'), content_length=0)
    assert b'' == dav.get('something.txt')
    f.write('aaa')
    dav.put('something.txt', f.open(), content_length=3)
    assert b'aaa' == dav.get('something.txt')

    dav.download('something.txt', str(f))
    assert 'aaa' == f.read()


def test_delete_exists(dav):
    assert dav.exists('something.txt')
    dav.delete('something.txt')
    assert not dav.exists('something.txt')
    pytest.raises(IOError, dav.delete, 'something.txt')
    dav.delete('something.txt', ignore_not_existing=True)


def test_mkdir_delete(dav):
    assert not dav.exists('somedir')
    pytest.raises(IOError, dav.put, 'somedir/sometext.txt', b'aaa')
    dav.mkdir('somedir')
    assert dav.exists('somedir')
    dav.put('somedir/sometext.txt', b'aaa')
    assert dav.exists('somedir/sometext.txt')
    pytest.raises(IOError, dav.mkdir, 'somedir')
    dav.mkdir('somedir', ignore_existing=True)
    dav.delete('somedir')
    assert not dav.exists('somedir')
    assert not dav.exists('somedir/sometext.txt')
