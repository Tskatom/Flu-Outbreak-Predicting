#!/usr/bin/env python
import os
import sys
import shutil
import tempfile
import zipfile
import modulefinder
from inspect import getsourcefile, getmodule

import disco.worker.classic.worker


def should_ignore(path, not_ignore_files):
    for ni in not_ignore_files:
        if ni in path:
            return False
    path_prefixes_to_ignore = ['/usr', '/opt']
    for prefix in path_prefixes_to_ignore:
        if path is not None and path.startswith(prefix):
            return True
    return False


def make_modules_zipfile(filename, not_ignore_files):
    zip_dir = tempfile.mkdtemp()
    zip_filename = os.path.join(zip_dir, 'modules.zip')
    finder = modulefinder.ModuleFinder()
    finder.run_script(filename)
    zf = zipfile.ZipFile(zip_filename, 'w')
    for name, mod in finder.modules.iteritems():
        if not mod.__file__:
            continue
        modfile = os.path.abspath(mod.__file__)
        if not should_ignore(modfile, not_ignore_files):
            if "__init__.py" not in modfile:
                for path in sys.path:
                    if os.path.commonprefix([path, modfile]) == path:
                        relative_path = os.path.relpath(modfile, path)
                        break
                zf.write(modfile, relative_path)
            else:
                moddir = os.path.dirname(modfile)
                for root, dirs, files in os.walk(moddir):
                    for file in files:
                        modfile = os.path.join(root, file)
                        for path in sys.path:
                            if os.path.commonprefix([path, modfile]) == path:
                                relative_path = os.path.relpath(modfile, path)
                                break
                        zf.write(modfile, relative_path)
    zf.close()
    return zip_dir, zip_filename


class Worker(disco.worker.classic.worker.Worker):
    not_ignore_files = []

    def jobdict(self, job, **jobargs):
        jobdict = super(Worker, self).jobdict(job, **jobargs)
        jobdict['prefix'] = "%s:%s" % (jobdict['owner'].split("@")[0],
                                       job.__class__.__name__)
        return jobdict

    def jobenvs(self, job, **jobargs):
        envs = super(Worker, self).jobenvs(job, **jobargs)
        envs['PYTHONPATH'] = ':'.join(('lib/modules.zip',
            envs.get('PYTHONPATH', '')))
        return envs

    def jobzip(self, job, **jobargs):
        def get(key):
            return self.getitem(key, job, jobargs)
        required_files = get('required_files')
        not_ignore_files = get('not_ignore_files')
        if not not_ignore_files:
            not_ignore_files = []

        job_path = getsourcefile(getmodule(job))
        zip_dir, modules_zipfile = make_modules_zipfile(job_path,
                                                       not_ignore_files)
        try:
            if isinstance(required_files, dict):
                zipdata = open(modules_zipfile, 'rb').read()
                required_files['lib/modules.zip'] = zipdata
            else:
                required_files.append(modules_zipfile)
            return super(Worker, self).jobzip(job, **jobargs)
        finally:
            if os.path.exists(zip_dir):
                shutil.rmtree(zip_dir)


if __name__ == '__main__':
    Worker.main()
