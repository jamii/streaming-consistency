Linux-only. Requires [nix](https://nixos.org/) to fetch dependencies.

```
nix-shell --pure --run './run.sh ../original-transactions.py'
```

You should eventually see `Done!`.

Check out the outputs in `./tmp`.