import itertools, os

from errno import EEXIST
from grp import getgrnam
from pwd import getpwnam
from resource_management.core.resources.system import Execute

def create_linux_user(user, group):
  try:
    getpwnam(user)
  except KeyError: 
    Execute('adduser ' + user)

  try: 
    getgrnam(group)
  except KeyError:
    Execute('groupadd ' + group)

def chown(path, user, group, recursive = False):
  uid = getpwnam(user).pw_uid
  gid = getgrnam(group).gr_gid
  if recursive:
    for dirpath, dnames, fnames in os.walk(path):
      for f in itertools.chain(dnames, fnames):
        os.chown(os.path.join(dirpath, f), uid, gid)
  os.chown(path, uid, gid)

def mkdirs(path):
  # See http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python/600612#answer-600612
  try:
    os.makedirs(path)
  except OSError as e:
    if e.errno != EEXIST or not os.path.isdir(path):
      raise
