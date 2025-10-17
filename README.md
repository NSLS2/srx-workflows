# SRX Workflows

Repository of workflows for the SRX beamline.

## Development

### Pre-commit

This project uses pre-commit to maintain code quality. To install and run
pre-commit locally:

```bash
# Install pre-commit hooks
pixi run pre-commit install

# Run on all files
pixi run pre-commit run --all-files

# Run on staged files only
pixi run pre-commit run

# Run on files changed in your branch (compared to main)
pixi run pre-commit run --from-ref main --to-ref HEAD
```
