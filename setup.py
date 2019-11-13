from setuptools import setup

sdict = {
    'name': 'norm',
    'version': '1.3',
    'description': 'Easy peasy sql generation',
    'long_description': "Really easy SQL generation",
    'url': 'http://github.com/justinvanwinkle',
    'download_url': 'https://github.com/justinvanwinkle/Norm',
    'author': 'Justin Van Winkle',
    'author_email': 'justin.vanwinkle@gmail.com',
    'maintainer': 'Justin Van Winkle',
    'maintainer_email': 'justin.vanwinkle@gmail.com',
    'keywords': ['Norm', 'sql'],
    'license': 'BSD',
    'packages': ['norm'],
    # 'test_suite' : 'tests.all_tests',
    'classifiers': [
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python']}

if __name__ == '__main__':
    setup(**sdict)
