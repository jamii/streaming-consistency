Linux-only. Requires [nix](https://nixos.org/) to fetch dependencies.

```
sudo mkdir /sys/fs/cgroup/cpu/jamii-consistency-demo
sudo chown -R $USER /sys/fs/cgroup/cpu/jamii-consistency-demo
NIXPKGS_ALLOW_INSECURE=1 nix-shell --pure --run ./run.sh
```