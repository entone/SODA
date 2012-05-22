# setup.py ---
#

from distutils.core import setup
from setuptools import find_packages

setup(name='SODA',
      version='0.0.1',
      author='Christopher Coté',
      packages=find_packages(),
      zip_safe=False,
      include_package_data=True,
)

#
# setup.py ends here
