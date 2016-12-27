from setuptools import setup
import os

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as f:
        required = f.read().splitlines()

setup(name='AwsElbLogUtil',
        version='0.1',
        description='Library to help downloading and parsing logs from AWS ELB',
        url='https://github.com/CurryEleison/AwsElbLogUtil',
        author='Thomas Petersen',
        author_email='petersen@temp.dk',
        license='MIT',
        packages=['AwsElbLogUtil'],
        install_requires=required,
        zip_safe=False)

