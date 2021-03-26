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

In another terminal do `tail -F ./tmp/outputs`. You should see something like this:

```
null	insert 84278,null
null	delete 84278,null
null	insert 84278,84278
null	insert 84279,null
null	delete 84279,null
null	insert 84279,84279
null	insert 84280,null
null	delete 84280,null
null	insert 84280,84280
null	insert 84281,null
```

