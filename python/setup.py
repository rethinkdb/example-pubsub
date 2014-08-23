from setuptools import setup


setup(
    name='repubsub',
    version='1.0.0',
    packages=['.'],
    description='A publish-subscribe library using RethinkDB',
    license='MIT',
    author='Josh Kuhn',
    author_email='josh@rethinkdb.com',
    url='http://rethinkdb.com/docs/publish-subscribe/python/',
    keywords=['pubsub', 'rethinkdb', 'publish', 'subscribe'],
    install_requires = ['rethinkdb>=1.13'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
)
