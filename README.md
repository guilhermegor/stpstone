# stpstone <img src="img/logo_stpstone.png" align="right" width="200" style="border-radius: 15px;" alt="stpstone">

[![Project Status: Active – The project has reached a stable, usable state and is being actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)
![PyPI Version](https://img.shields.io/pypi/v/stpstone)
![Linting](https://img.shields.io/badge/linting-flake8%20+%20isort-blue)
![Security](https://img.shields.io/badge/security-bandit-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![PyPI Downloads](https://img.shields.io/pypi/dm/stpstone?color=teal)
![Open Issues](https://img.shields.io/github/issues/guilhermegor/stpstone)
![Contributions Welcome](https://img.shields.io/badge/contributions-welcome-darkgreen.svg)

**stpstone** (short for *stepping stone*) is a Python framework designed for ingesting, processing, and analyzing structured and unstructured financial data. It provides tools for ETL (Extract, Transform, Load), quantitative analysis, and derivatives pricing, optimized for financial market applications.

## ✨ Key Features

- **Data Extraction**: Fetch market data from multiple sources (B3, CVM, BACEN/Olinda)
- **Quantitative Methods**:
  - Portfolio optimization
  - Risk management
  - Financial modeling
- **Derivatives Pricing**:
  - Closed-form solutions (e.g., Black-Scholes)
  - Iterative methods (e.g., Binomial Tree)
- **Data Processing**: Clean, structure, and transform raw financial data
- **Database Integration**: Seamlessly load data into PostgreSQL, MySQL, and SQLite

## 🚀 Getting Started

### Prerequisites:

- **Python** ^3.12
- **Pyenv**
- **Make** (optional, below are some benefits of usage)

    | Category	| Example Targets | Benefit |
    | --------- | --------------- | ------- |
    | Package Lifecycle	| `build_package_pypi_org`, `clean_dist` | Automated PyPI publishing |
    | Git Workflows	| `git_pull_force`, `gh_protect_main` | Enforces branch policies |
    | CI/CD	| `gh_actions_local_tests`	| Local pipeline validation |
    | Code Generation | `ingestion_concrete_creator` | Factory pattern automation |
    | Dev Environment | `vscode_install_extensions` | Consistent IDE setup |

### Installation Guide:

-  **Option 1: Pip Install (Recommended)**
📌 Available on PyPI

    <pre style="font-size: 12px;"><code>
    #!/bin/bash
    pip install stpstone
    poetry install --no-root
    poetry shell
    </code></pre>

- **Option 2: Build from Source**

    <pre style="font-size: 12px;"><code>
    #!/bin/bash
    git clone https://github.com/guilhermegor/stpstone.git
    cd stpstone
    pyenv install 3.12.8
    pyenv local 3.12.8
    poetry install --no-root
    poetry shell
    </code></pre>

- **Setting Up Make (Optional for Build Automation)**

    | Platform | Instructions |
    |----------|--------------|
    | **Windows** | Install via [MinGW](https://sourceforge.net/projects/mingw/)<br>1. Download MinGW installer<br>2. Select `mingw32-make` during installation<br>3. Add `C:\MinGW\bin` to PATH<br>4. ```mingw32-make --version```|
    | **MacOS** | Pre-installed or via Xcode CLI tools:<br>`xcode-select --install` |
    | **Linux** | Install via package manager:<br>`sudo apt-get install build-essential` |


- **Python Kernel Versioning (Pyenv)**: [pyenv instructions to installation](https://github.com/pyenv/pyenv)

## 🧪 Running Tests

Execute unit and integration tests:

<pre style="font-size: 12px;"><code>
#!/bin/bash
poetry run python -m unittest discover -s tests/unit -p "*.py" -v
poetry run python -m unittest discover -s tests/integration -p "*.py" -v
</code></pre>

## 📂 Project Structure

<pre style="font-size: 12px;"><code>
stpstone/
│
├── 📁 .github/
│   ├── 📁 workflows/         # GitHub Actions CI/CD pipelines
│   ├── 📜 CODEOWNERS         # Code ownership definitions
│   └── 📜 PULL_REQUEST_TEMPLATE.md  # PR template
│
├── 📁 .vscode/               # VSCode configuration
│   └── ⚙️ settings.json      # Editor preferences and extensions
│
├── 📁 cli/                   # Command Line Interface components
│
├── 📁 data/                  # Data storage and management
│
├── 📁 docs/                  # Project documentation
│
├── 📁 examples/              # Example implementations
│
├── 📁 img/                   # Visual assets
│
├── 📁 stpstone/              # Core Python package
│   ├── 📁 _config/           # Configuration management
│   ├── 📁 analysis/          # Analytical components
│   ├── 📁 dsa/               # Data structures & algorithms
│   ├── 📁 ingestion/         # Data ingestion pipelines
│   ├── 📁 transformations/   # Data transformation logic
│   ├── 📁 utils/             # Shared utilities
│   └── 🐍 __init__.py        # Package initialization
│
├── 📁 tests/                 # Test suites
│   ├── 📁 unit/              # Unit tests
│   ├── 📁 integration/       # Integration tests
│   └── 📁 performance/       # Performance benchmarking
│
├── 📜 .gitignore             # Git ignore patterns
├── ⚙️ .pre-commit-config.yaml # Pre-commit hook configurations
├── � .python-version        # Pyenv version specification
├── 📜 LICENSE               # MIT License file
├── ⚙️ Makefile              # Automation tasks
├── 📦 poetry.lock           # Exact dependency versions
├── ⚙️ pyproject.toml        # Project metadata and dependencies
├── 📖 README.md             # Project overview
├── 📦 requirements.txt      # Production dependencies
├── 🔧 requirements-dev.txt  # Development dependencies
└── 💻 requirements-venv.txt # Virtual environment setup
</code></pre>

## 👨‍💻 Authors

**Guilherme Rodrigues**  
[![GitHub](https://img.shields.io/badge/GitHub-guilhermegor-181717?style=flat&logo=github)](https://github.com/guilhermegor)  
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Guilherme_Rodrigues-0077B5?style=flat&logo=linkedin)](https://www.linkedin.com/in/guilhermegor/)

## 📜 License

This project is licensed under the MIT License - see LICENSE for details.

## 🙌 Acknowledgments

* Inspired by open-source financial libraries and tools

* Documentation template: [PurpleBooth/README-Template.md](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)

* Special thanks to Python community

## 🔗 Useful Links

* [GitHub Repository](https://github.com/guilhermegor/stpstone)

* [Issue Tracker](https://github.com/guilhermegor/stpstone/issues)