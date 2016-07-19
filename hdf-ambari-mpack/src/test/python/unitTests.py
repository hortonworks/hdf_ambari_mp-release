#!/usr/bin/env python

import os, sys, unittest

def up(path, num = 1):
  for i in range(num):
    path = os.path.dirname(path)
  return os.path.abspath(path)

def run_test_folder(runner, base_dir, python_test_dir, test_dir):
  orig_sys_path = [ entry for entry in sys.path ]
  try:
    test_relpath = os.path.relpath(test_dir, python_test_dir)
    resources_dir = os.path.abspath(os.path.join(base_dir, 'src/main/resources'))
    source_dir = os.path.join(resources_dir, test_relpath)
    sys.path.append(source_dir)
    sys.path.append(test_dir)
    test_files = []
    for item in os.listdir(test_dir):
      if os.path.isfile(os.path.join(test_dir, item)) and item.endswith('_test.py'):
        test_files.append(item[:-3])
    suite = unittest.TestLoader().loadTestsFromNames(test_files)
    return runner.run(unittest.TestSuite([suite])).wasSuccessful()
  finally:
    sys.path = orig_sys_path

def run_tests():
  this_file = os.path.abspath(__file__)
  python_test_dir = up(this_file)
  base_dir = up(python_test_dir, 3)

  test_dirs = set([])

  for dirpath, dnames, fnames in os.walk(python_test_dir):
    for fname in fnames:
      fpath = os.path.abspath(os.path.join(dirpath, fname))
      if fpath != this_file and fname.endswith('.py'):
        test_dirs.add(up(fpath))

  runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
  was_successful = True
  for test_dir in test_dirs:
    was_successful = was_successful and run_test_folder(runner, base_dir, python_test_dir, test_dir)
  return was_successful

if __name__ == '__main__':
  if not run_tests():
    raise Exception('Failed tests')
