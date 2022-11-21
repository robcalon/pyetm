from setuptools import setup, find_packages

with open('README.md', 'rb') as file:
    README = file.read().decode('UTF-8')

# with open('requirements.txt') as file:
#     requires = [[l.strip() for l in file.readlines() if not l.startswith("#")]]
    
setup(
    name='pyETM',
    version='0.8.1',    
    description='Python-ETM Connector',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/robcalon/pyetm',
    author='Rob Calon',
    author_email='robcalon@protonmail.com',
    license='EUPL-1.2',
    python_requires='>=3.7',
    install_requires=["requests>=2.26", "pandas>=1.3"],
    extras_require={
        "async": ["aiohttp>=3.8.1"],
        "excel": ["xlsxwriter>=3.0"],
        "all": ["aiohttp>=3.8.1", "xlsxwriter>=3.0"],
        },
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
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
