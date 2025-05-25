from setuptools import setup, find_packages


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# available classifiers: https://pypi.org/classifiers/
list_classifiers = [
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "Intended Audience :: Financial and Insurance Industry",
    "Intended Audience :: Science/Research",
    "Topic :: Office/Business :: Financial",
    "Topic :: Office/Business :: Financial :: Spreadsheet",
    "Topic :: Office/Business :: Office Suites",
    "Topic :: Office/Business :: Scheduling",
    "Topic :: Other/Nonlisted Topic",
    "Topic :: Scientific/Engineering :: Mathematics",
]

setup(
    name="stpstone",
    version="2.0.0",
    description="Solid financial ETL, analytics and utils with support to global markets.",
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/guilhermegor/stpstone",
    author="guilhermegor",
    author_email="github.bucked794@silomails.com",
    license="MIT",
    classifiers=list_classifiers,
    keywords="stpstone, python, financial, data, utils, analytics, ingestion, b3, cvm, exchange, derivatives, quantitative, risk, portfolio, fixed income, options, futures, market data, macroeconomic, scraping, statistics, time series, cryptocurrency, brazilian markets, pricing models, financial mathematics",
    install_requires=[
        'poetry==2.1.2 ; python_full_version >= "3.12.8" and python_version < "3.14"',
    ],
    python_requires=">=3.12.8",
    project_urls={
        "Bug Reports": "https://github.com/guilhermegor/stpstone/issues",
        "Source": "https://github.com/guilhermegor/stpstone",
    },
)