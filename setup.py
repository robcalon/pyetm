from setuptools import setup, find_packages

with open('README.md', 'rb') as file:
    README = file.read().decode('UTF-8')

setup(
    name='pyETM',
    version='0.2.0',    
    description='Python-ETM Connector',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/robcalon/pyetm',
    author='Rob Calon',
    author_email='robcalon@protonmail.com',
    license='MIT',
    python_requires='>=3.7',
    install_requires=['numpy>=0.11',
                      'pandas>=0.17',
                      'aiohttp',
                      'nest_asyncio'],
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Development Status :: 1 - Planning',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',        
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ],
)