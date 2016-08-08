"""
Basic WebDAV implementation

Reason this exists: easywebdav is not Python 3 compatible, webdavclient is
flaky and not compatible with (at least) Beehub.
"""
import requests


def verify(acceptable_statuses, response, message):
    """
    Verify that the request gave an acceptable status_code and raises an
    IOError otherwise.
    """
    if response.status_code not in acceptable_statuses:
        header_str = '\n'.join(['{0}: {1}'.format(k, v)
                                for k, v in response.headers])
        raise IOError('{0}: HTTP status code {1}\nHeaders:\n{2}'
                      .format(message, response.status_code, header_str))


class RestRequests(object):
    """ Rest requests with a base URL. """
    def __init__(self, base_url, auth=None, **kwargs):
        """
        @param base_url: base url of the service
        @param auth: requests authentication tuple
        @param kwargs: kwargs to pass on to the requests.request() function
        """
        self.base_url = base_url.rstrip('/')
        self.auth = auth
        self.kwargs = kwargs

    def exists(self, path, **kwargs):
        """ Whether the path exists. """
        kwargs.update(self.kwargs)
        response = requests.head(self.path_to_url(path), auth=self.auth,
                                 **kwargs)
        return response.status_code == 200

    def put(self, path, data, content_type=None, **kwargs):
        """ Put bytes or a file pointer. """
        kwargs.update(self.kwargs)
        if content_type is None:
            headers = None
        else:
            headers = {'content-type': content_type}
        response = requests.put(self.path_to_url(path), data=data,
                                auth=self.auth, headers=headers, **kwargs)
        verify((201,), response, 'Failed to upload file {0}'.format(path))

    def mkdir(self, path, ignore_existing=False, **kwargs):
        """
        Make a new directory. Set ignore_existing to not throw an error if
        the directory already exists.
        """
        kwargs.update(self.kwargs)
        response = requests.request('MKCOL', self.path_to_url(path),
                                    auth=self.auth)
        if ignore_existing:
            acceptable_status = 201, 405
        else:
            acceptable_status = 201,
        verify(acceptable_status, response,
               'Failed to create directory {0}'.format(path))

    def delete(self, path, ignore_not_existing=False, **kwargs):
        """
        Make a new directory. Set ignore_existing to not throw an error if
        the directory already exists.
        """
        kwargs.update(self.kwargs)
        response = requests.delete(self.path_to_url(path), auth=self.auth,
                                   **kwargs)
        if ignore_not_existing:
            acceptable_status = 204, 404
        else:
            acceptable_status = 204,
        verify(acceptable_status, response,
               'Failed to delete {0}'.format(path))

    def get(self, path, **kwargs):
        """ Get path contents as bytes. """
        kwargs.update(self.kwargs)
        response = requests.get(self.path_to_url(path), auth=self.auth,
                                **kwargs)
        verify((200,), response, 'Failed to get file {0}'.format(path))
        return response.content

    def download(self, path, file_path, chunk_size=1024*1024, **kwargs):
        """ Download path to file_path. """
        kwargs.update(self.kwargs)
        response = requests.get(self.path_to_url(path), stream=True,
                                auth=self.auth, **kwargs)

        verify((200,), response, 'Failed to get file {0}'.format(path))
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)

    def path_to_url(self, path):
        """ Given a path relative to the base_url, returns the full URL."""
        return '{0}/{1}'.format(self.base_url, path.lstrip('/'))

    def url_to_path(self, url):
        """
        Given an URL prefixed with the base_url, return the relative path.
        """
        if not url.startswith(self.base_url):
            raise ValueError('URL {0} cannot be translated to webdav at {1}'
                             .format(url, self.base_url))
        return url[len(self.base_url):].lstrip('/')