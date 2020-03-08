# pyre-fixme[21]: Could not find `setuptools`.
from setuptools import setup, find_packages

setup(packages=find_packages(), install_requires=["xlsxwriter", "pandas", "pyre-check"])
