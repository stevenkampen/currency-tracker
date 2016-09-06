import sys, os

def patched_expanduser(path):
    return os.path

os.path.expanduser = patched_expanduser

sys.path.append(os.path.join(os.path.dirname(__file__), 'lib'))

