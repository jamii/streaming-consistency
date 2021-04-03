Linux-only. Requires [nix](https://nixos.org/) to fetch dependencies.

```
sudo mkdir /sys/fs/cgroup/cpu/jamii-consistency-demo
sudo chown -R $USER /sys/fs/cgroup/cpu/jamii-consistency-demo
nix-shell --pure --run ./run.sh
```

You should eventually see something like this:

```
All systems go. Hit ctrl-c when you're ready to shut everything down.
```

Leave that running for now.

---

In another terminal check out the outputs: