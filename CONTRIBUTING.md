# Contribution Guidelines

Thank you for considering contributing to our project! Please take a moment to review these guidelines to ensure a smooth collaboration.

## Development Conventions

### Variable Naming Standards

We use the following standardized variable naming conventions:

| Variable        | Description |
|-----------------|-------------|
| `int_cddr`      | Integer calendar days redemption period (funding to delivery) |
| `int_wddr`      | Integer working days redemption period (funding to delivery) |
| `int_cddt`      | Integer calendar days total investment period (funding to maturity) |
| `int_wddt`      | Integer working days total investment period (funding to maturity) |
| `int_cddm`      | Integer calendar days per month (convention: 30) |
| `int_wddm`      | Integer working days per month (convention: 22) |
| `int_cddy`      | Integer calendar days per year (convention: 365) |
| `int_wddy`      | Integer working days per year (convention: 252) |
| `int_cd_bef`    | Integer calendar days before (convention: 1) |
| `int_wd_bef`    | Integer working days before (convention: 1) |
| `int_cd_aft`    | Integer calendar days after (convention: 1) |
| `int_wd_aft`    | Integer working days after (convention: 1) |
| `int_cd_cap`    | Integer calendar days capitalization |
| `int_wd_cap`    | Integer working days capitalization |
| `int_nper_cd`   | Integer number of periods (calendar days) |
| `int_nper_wd`   | Integer number of periods (working days) |
| `float_pv`      | Present value of instrument notional (negative for purchasing, positive for selling) |
| `float_fv`      | Future value of instrument notional (negative for purchasing, positive for selling) |
| `float_notional`| Priced value from contract in absolute terms |
| `float_size`    | Size of instrument contract |
| `float_xcg`     | Exchange value for instrument pricing |
| `float_qty`     | Quantity of instruments (positive for purchasing, negative for selling) |
| `dt_ref`        | Reference date |
| `dt_xpt`        | Maturity date |
| `dt_rdm`        | Redemption date |
| `int_n_smp`     | Number of samples |

## Branching Strategy

### Branch Naming Convention

All branches must follow the pattern: `<purpose>/<branch-task>`

**Available purposes:**

| Purpose         | Format                      | When to Use |
|-----------------|-----------------------------|-------------|
| Feature         | `feature/<name>` or `feat/<name>` | New functionality |
| Bugfix          | `bugfix/<description>` or `fix/<description>` | Bug resolution |
| Hotfix          | `hotfix/<description>` | Critical production fixes |
| Release         | `release/<version>` | Version preparation |
| Documentation   | `docs/<description>` | Documentation updates |
| Refactor        | `refactor/<description>` | Code improvements |
| Chore           | `chore/<description>` | Project maintenance tasks |

**Examples:**
- `feat/user-authentication`
- `fix/login-validation-issue`
- `docs/update-api-reference`

## Commit Message Standards

All commits must follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```bash
<type>(<scope>): <description>
text
```


**Available types:**

| Type       | Purpose |
|------------|---------|
| `build`    | Build system or dependency changes |
| `ci`       | CI configuration changes |
| `docs`     | Documentation updates |
| `feat`     | New features |
| `fix`      | Bug fixes |
| `perf`     | Performance improvements |
| `refactor` | Code restructuring |
| `style`    | Formatting changes |
| `test`     | Test additions/modifications |
| `chore`    | Maintenance tasks |
| `revert`   | Reverting changes |
| `bump`     | Version updates |

**Examples:**
- `feat(auth): implement OAuth2 integration`
- `fix(calculations): correct rounding errors in tax computation`
- `docs(readme): add installation instructions`

## Development Setup

1. **Dependency Management**:
   - Use Poetry (version specified in `requirements.txt`)
   - Avoid adding redundant libraries - check `pyproject.toml` first
   - Run `poetry install` to set up the development environment

2. **Pre-commit Hooks**:
   - Install with `make precommit_update`
   - This enables automatic:
     - Code linting
     - Formatting
     - Security vulnerability scanning
     - Large file detection
     - Secret key detection

3. **Testing**:
   - Ensure all new code has corresponding tests
   - Run tests locally before pushing
   - Maintain 100% test coverage for critical components

## Pull Request Process

1. **Create an Issue First**:
   - Check existing issues at [GitHub Issues](https://github.com/guilhermegor/stpstone/issues)
   - Open a new issue if none exists for your work

2. **Opening a PR**:
   - Fill out the PR template completely
   - Include:
     - Detailed description
     - Changes made
     - Testing performed
     - Documentation updates
     - Any technical debt created

3. **Code Review**:
   - Expect constructive feedback
   - Address all review comments
   - Update documentation as needed
   - Keep commits logically organized

4. **Merge Approval**:
   - Requires at least one approval
   - All tests must pass
   - Code coverage should not decrease
   - Documentation must be updated

## Best Practices

- **Keep branches focused** - one feature/bugfix per branch
- **Make small, frequent commits** - easier to review
- **Write descriptive commit messages** - explain why, not just what
- **Update documentation** - when adding new features
- **Follow existing patterns** - maintain consistency
- **Communicate early** - if you're stuck or need clarification

We appreciate your contributions and look forward to collaborating with you!