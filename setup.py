from setuptools import setup, find_packages

descr = open('README.md', "r")
rqr = open("requirements.txt", "r")

setup(
    name='KP-DataProcessing',
    version='1.0.0',
    author='Beata Wysocka',
    author_email='beata.wysocka@capgemini.com',
    packages=find_packages(exclude=['tests']),
    description='Package for dedicated data processing within company dealing with Bitcoin trading',
    long_description=descr.read(),
    install_requires=rqr.read().split("\n")
)

descr.close()
rqr.close()
