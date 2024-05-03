#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='moneybot',
    version='0.9',
    packages=find_packages(),
    url='https://t.me/prozorrowatch',
    license='MIT',
    install_requires = ['pyTelegramBotAPI', "requests==2.31.0", 'ujson', 'urllib3'],
    author='rino',
    author_email='mavladi@gmail.com',
    description='Loads new procurements form Prozorro and publish top of them into your Telegram channel',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Environment :: Console',
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Topic :: Office/Business :: Financial'
    ],
)
