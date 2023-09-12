from setuptools import setup, find_packages

rqs_file = open("requirements.txt", "r")
rq = rqs_file.read().split("\n")
rqs_file.close()

setup(
   name='KP-DataProcessing',
   version='1.0.0',
   author='Beata Wysocka',
   author_email='beata.wysocka@capgemini.com',
   packages=find_packages(exclude=['tests']),
   description='Package for dedicated data processing within company dealing with Bitcoin trading',
   long_description=open('README.txt').read(),
   install_requires = rqs,
)
