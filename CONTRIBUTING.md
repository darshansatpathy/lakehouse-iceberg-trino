# Contributing

```bash
docker compose up -d && make wait
python -m venv .venv && . .venv/bin/activate
make install
LH_INTEGRATION=1 make check
make verify
```

## Changing a model

1. Change the model in `transform/`.
2. If it changes results, change `Reference` in `src/lakehouse/generator.py` to match, and add
   or adjust a check in `src/lakehouse/verify.py`.
3. Run `make verify` and commit the regenerated `docs/verification.md`.

## Changing the stack

Pin every image to a version. After changing one, run the whole verification and the benchmark,
and say in the pull request what changed in the reports.
