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

__accepted_transactions__. In about 1 in 5 runs, some of these are missing. 

```
$ wc -l tmp/*transactions
 999514 tmp/accepted_transactions
 1000000 tmp/transactions
 1999514 total

$ cat tmp/transactions | cut -d',' -f 1 | cut -d' ' -f 2 > in

$ cat tmp/accepted_transactions | cut -d',' -f 1 | cut -d':' -f 2 > out

$ diff in out | wc -l
 487

$ diff in out | head
 25313,25798d25312
 < 25312
 < 25313
 < 25314
 < 25315
 < 25316
 < 25317
 < 25318
 < 25319
 < 25320
 
$ diff in out | tail
 < 25788
 < 25789
 < 25790
 < 25791
 < 25792
 < 25793
 < 25794
 < 25795
 < 25796
 < 25797
```

On uncommenting any of the other examples in src/main/java/Demo.java, I find that no output is produced at all, except for on one run out of many on a very slow machine. That strongly suggests to me that there is a race somewhere, rather than a logic bug in the examples.