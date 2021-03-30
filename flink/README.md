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

In another terminal check out the outputs:

__./tmp/outer_join_with_time__. Some rows have nulls which are never retracted.

__./tmp/outer_join_without_time__. Some rows have nulls which are immediately retracted.

If you don't see a particular failure, hit ctrl-c in the first terminal and try again. They're all pretty racy.