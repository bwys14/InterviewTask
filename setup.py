from setuptools import setup, find_packages

setup(
   name='KP-DataProcessing',
   version='1.0.0',
   author='Beata Wysocka',
   author_email='beata.wysocka@capgemini.com',
   packages=find_packages(exclude=['tests']),
   description='Package for dedicated data processing within company dealing with Bitcoin trading',
   long_description=open('README.MD', "r").read(),
   install_requires = open("requirements.txt", "r").read().split("\n"),
)
