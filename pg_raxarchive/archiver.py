import os
import gzip
import shutil
import logging
import subprocess

from multiprocessing.dummy import Pool as ThreadPool


try:
    from cStringIO import StringIO
    StringIO  # XXX: pyflakes workaround
except ImportError:
    from StringIO import StringIO
from tempfile import NamedTemporaryFile
from contextlib import closing, contextmanager

import pyrax
import pyrax.exceptions as exc


@contextmanager
def removing_dir(dirname):
    try:
        yield dirname
    finally:
        shutil.rmtree(dirname)


@contextmanager
def atomicfilewriter(filename, mode='wb'):
    try:

        tmpfilename = os.path.join(
            os.path.dirname(filename),
            '.tmp-{}'.format(os.path.basename(filename)))
        fout = open(tmpfilename, mode)
        yield fout
    except:
        fout.close()
        os.unlink(tmpfilename)
    else:
        fout.close()
        os.rename(tmpfilename, filename)


def iterchunks(stream):
    while True:
        data = stream.read(2 ** 20)
        if not data:
            break
        yield data


class FileNotFound(RuntimeError):
    pass


class PGRaxArchiver(object):
    def __init__(self, filename, region, container_name, use_public):
        pyrax.set_setting('identity_type', 'rackspace')
        pyrax.set_credential_file(filename)
        self.cf = pyrax.connect_to_cloudfiles(region=region, public=use_public)
        self.cnt = self.cf.create_container(container_name)

    def upload(self, src_name, dst_name, compress=True, use_gzip=False):
        if compress:
            fout = NamedTemporaryFile(suffix='.gz', mode='wb', delete=False)
            try:
                if use_gzip:
                    logging.debug('Compressing file %s with gzip...', src_name)
                    p = subprocess.Popen(["gzip", '-c', src_name], stdout=fout)
                    assert p.wait() == 0, 'Gzip compression failed'
                    fout.close()
                    return self._upload(fout.name, dst_name + '.gz')
                else:
                    fout.close()
                    logging.debug('Compressing file %s...', src_name)
                    with \
                            open(src_name, 'rb') as fin, \
                            closing(gzip.GzipFile(fout.name, mode='wb')) as gzout:
                        for chunk in iterchunks(fin):
                            gzout.write(chunk)
                    return self._upload(fout.name, dst_name + '.gz')
            finally:
                fout.unlink(fout.name)
        else:
            self._upload(src_name, dst_name)

    def _upload(self, filename, obj_name):
        logging.debug('Uploading file %s...', obj_name)
        self.cnt.upload_file(filename, obj_name=obj_name, return_none=True)

    def _exists_remote(self, src_name):
        try:
            self.cnt.get_object(src_name)
            return True
        except exc.NoSuchObject:
            return False

    def _get_cache_path(self, src_name, dst_name):
        dst_direcotry = os.path.join(*os.path.split(dst_name)[:-1])
        return os.path.join(dst_direcotry, src_name + '.tmp')

    def _pop_from_cache(self, src_name, dst_name):
        cached_path = self._get_cache_path(src_name, dst_name)
        try:
            data = open(cached_path).read()
            os.remove(cached_path)
            return data
        except Exception, exc:
            logging.debug('Impossible to open cached file %s (%s)', cached_path, exc)

    def _fetch_files(self, src_name, dst_name, compress, prefetch):
        pool = ThreadPool(prefetch or 1)
        tasks = []
        for i in range(prefetch):
            source = hex(int(src_name, 16) + i).rstrip("L").lstrip("0x").upper().zfill(len(src_name))
            tasks.append(pool.apply_async(self._get_from_remote,
                                          (source, dst_name, compress)))

        for task in tasks:
            source, data = task.get()
            cache_filename = self._get_cache_path(source, dst_name)
            with open(cache_filename, 'wb') as cache_file:
                cache_file.write(data)
                logging.debug('Saved for later %s', cache_filename)

    def _get_from_remote(self, src_name, dst_name, compress='auto'):
        # XXX: use external memory instead of store everything in RAM
        _src_name = src_name
        if compress == 'auto':
            if self._exists_remote(src_name + '.gz'):
                compress = True
                _src_name = _src_name + '.gz'
            elif self._exists_remote(_src_name):
                compress = False
            else:
                raise FileNotFound(_src_name)

        logging.debug('Fetching file %s...', _src_name)
        data = self.cnt.fetch_object(_src_name)

        if compress is True:
            data = self._decompress_stream(data)

        return src_name, data

    def _decompress_stream(self, data):
        logging.debug('Decompressing...')
        stream = StringIO(data)
        with closing(gzip.GzipFile(fileobj=stream, mode='rb')) as fin:
            return fin.read()

    def download(self, src_name, dst_name, compress='auto', prefetch=0):
        data = self._pop_from_cache(src_name, dst_name)
        if not data:
            self._fetch_files(src_name, dst_name, compress=compress, prefetch=prefetch)
            data = self._pop_from_cache(src_name, dst_name)

        logging.debug('Writing file %s...', dst_name)
        with atomicfilewriter(dst_name, 'wb') as fout:
            fout.write(data)

    def cleanup(self, filename):
        names = self.cnt.get_object_names()

        def stripgz(s):
            if s.endswith('.gz'):
                return s[:-3]
            return s

        def normalize(name):
            return name.partition('.')[0]

        uncompressed_names = {stripgz(k): k for k in names}

        filename = normalize(filename)
        removing_names = [uncompressed_names[k] for k in uncompressed_names
                          if normalize(k) < filename]

        for obj_name in removing_names:
            logging.debug('Removing file %s...', obj_name)
            self.cnt.delete_object(obj_name)
