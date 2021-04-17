__INCOMPLETE__

Linux-only. Requires [nix](https://nixos.org/) to fetch dependencies.

```
NIXPKGS_ALLOW_INSECURE=1 nix-shell --pure --run './run.sh ../original-transactions.py'
```