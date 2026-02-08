# Development Notes

## Getting Started

You will need [Pixi](https://pixi.sh/latest/) to build this project.

```bash
pixi install
```

## Testing

Run the unit tests:

```bash
pixi run test
```

Run with verbose output:

```bash
pixi run test-v
```

Run with coverage report:

```bash
pixi run test-cov
```

Lint:

```bash
pixi run lint
```

Format:

```bash
pixi run fmt
```

Lint and test together:

```bash
pixi run check
```

### Integration Tests

Integration tests submit real jobs to an LSF cluster and are skipped by default. See [integration-tests.md](integration-tests.md) for setup and usage.

```bash
pixi run test-integration
```

## Release

First, increment the version in `pyproject.toml` and push it to GitHub. Create a *Release* there and then publish it to PyPI as follows.

To create a Python source package (`.tar.gz`) and the binary package (`.whl`) in the `dist/` directory, do:

```bash
pixi run -e release pypi-build
```

To upload the package to PyPI, you'll need one of the project owners to add you as a collaborator. After setting up your access token, do:

```bash
pixi run -e release pypi-upload
```
