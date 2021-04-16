Linux-only. Requires [nix](https://nixos.org/) to fetch dependencies.

```
nix-shell --pure --command './run.sh ../original-transactions.py'
```

You should eventually see something like this:

```
All systems go. Hit ctrl-c when you're ready to shut everything down.
```

Leave that running for now and check out the outputs in `./tmp` in another terminal.