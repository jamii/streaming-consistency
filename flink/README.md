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

__accepted_transactions__. I expected some of these to be dropped by the 5s watermark. Not sure what's up with that.

``` bash
$ wc -l tmp/transactions
100000 tmp/transactions
$ wc -l tmp/accepted_transactions
100000 tmp/accepted_transactions
$ tail tmp/transactions
 99990, 6, 1, 1, 2021-01-01 00:00:56
 99991, 0, 9, 1, 2021-01-01 00:00:49
 99992, 1, 8, 1, 2021-01-01 00:00:49
 99993, 6, 6, 1, 2021-01-01 00:00:52
 99994, 2, 2, 1, 2021-01-01 00:00:51
 99995, 8, 9, 1, 2021-01-01 00:00:57
 99996, 3, 3, 1, 2021-01-01 00:00:58
 99997, 7, 2, 1, 2021-01-01 00:00:58
 99998, 5, 0, 1, 2021-01-01 00:00:54
 99999, 2, 1, 1, 2021-01-01 00:00:52
```

__outer_join_with_time__. Some rows have nulls which are never retracted ([FLINK-22075](https://issues.apache.org/jira/browse/FLINK-22075?page=com.atlassian.jira.plugin.system.issuetabpanels%3Aall-tabpanel)).

``` bash
$ grep -c insert tmp/outer_join_with_time
100000
$ grep -c delete tmp/outer_join_with_time
0
$ grep -c null tmp/outer_join_with_time
15405
```

__outer_join_without_time__. Some rows have nulls which are immediately retracted.

``` bash
$ grep -c insert tmp/outer_join_without_time
199674
$ grep -c delete tmp/outer_join_without_time
99674
$ grep insert tmp/outer_join_without_time | grep -c null
99674
```

__sums__. There are only 60 distinct timestamps in the inputs, so there should be at most 60 inserts from this aggregate.

``` bash
$ wc -l tmp/sums
199999 tmp/sums
```

__total/total2__. Money is only moving around so total amount should always be 0. Each transaction is for $1. 

``` bash
$ grep -c insert tmp/total
378066
$ grep -c 'insert 0.0' tmp/total
275
$ tail tmp/total
delete -179.0
insert 2.0
delete 2.0
insert 64.0
delete 64.0
insert 1.0
delete 1.0
insert 64.0
delete 64.0
insert 0.0
$ cut -d' ' -f 2 tmp/total | sort -h | head -n 1
-360.0
$ cut -d' ' -f 2 tmp/total | sort -h | tail -n 1
914.0
```

---

If you don't see a particular failure, hit ctrl-c in the first terminal and try again. They're all pretty racy.