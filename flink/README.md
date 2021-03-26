Linux-only. Requires [nix](https://nixos.org/) to fetch dependencies.

```
sudo mkdir /sys/fs/cgroup/cpu/jamii-consistency-demo
sudo chown -R $USER /sys/fs/cgroup/cpu/jamii-consistency-demo
nix-shell --pure --run ./run.sh
```

You should eventually see something like this:

```
...
null	delete 99998,null
null	insert 99998,99998
null	insert 99999,null
null	delete 99999,null
null	insert 99999,99999
```

Hit ctrl-c to shut everything down.