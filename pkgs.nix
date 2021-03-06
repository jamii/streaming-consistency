# pin package index to nixos-20.09
# TODO will this fail with patch releases?
import (builtins.fetchTarball {
    name = "nixos-20.09";
    url = "https://github.com/NixOS/nixpkgs/archive/20.09.tar.gz";
    sha256 = "1wg61h4gndm3vcprdcg7rc4s1v3jkm5xd7lw8r2f67w502y94gcy";
}) {
    overlays = [(self: super: {
    
        # override kafka to v2.7.0
        apacheKafka = super.apacheKafka.overrideAttrs (oldAttrs: rec {
            version = "2.7.0";
            src = self.fetchurl {
                url = "mirror://apache/kafka/${version}/kafka_2.13-${version}.tgz";
                sha256 = "143zrghrq40lrwacwyzqkyzg4asax7kxg9cgnkn2z83n6rv4pn0x";
            };
            # install zookeeper scripts
            installPhase = ''
                mkdir -p $out/bin
                cp bin/zookeeper* $out/bin
            '' + oldAttrs.installPhase;
        });
        
        # override flink to v1.12.2
        flink = super.flink.overrideAttrs (oldAttrs: rec {
            flinkVersion = "1.12.2";
            name = "flink-${flinkVersion}";
            src = self.fetchurl {
                url = "mirror://apache/flink/${name}/${name}-bin-scala_2.11.tgz";
                sha256 = "17c2v185m3q58dcwvpyzgaymf7767m8dap0xb318ijphb9sapvpk";
            };
            installPhase = ''
                rm bin/*.bat || true
                
                mkdir -p $out/bin $out/opt/flink
                mv * $out/opt/flink/
                makeWrapper $out/opt/flink/bin/flink $out/bin/flink \
                    --prefix PATH : ${super.jre}/bin
                
                cat <<EOF >> $out/opt/flink/conf/flink-conf.yaml
                env.java.home: ${super.jre}
                io.tmp.dirs: ./tmp
                env.log.dir: ./tmp/logs/flink/
                jobmanager.memory.process.size: 2000m
                taskmanager.memory.process.size: 32000m
                EOF
            '';
        });
        
        # override spark to v3.1.1
        spark = super.spark.overrideAttrs (oldAttrs: rec {
            version = "3.1.1";
            buildInputs = oldAttrs.buildInputs ++ [ super.procps super.nettools ];
            src = super.fetchzip {
                url    = "mirror://apache/spark/spark-${version}/spark-${version}-bin-without-hadoop.tgz";
                sha256 = "1l6jwf79wc1wbr8h1fgggd9fmn173bmj6lkjmn0m6a4xd7nk6pv7";
            };
        });
        
        # download materialized v0.7.1
        materialized = super.stdenv.mkDerivation {
            name = "materialized";
            src = fetchTarball {
                url = "https://downloads.mtrlz.dev/materialized-v0.7.1-x86_64-unknown-linux-gnu.tar.gz";
                sha256 = "1lhx0fvjsqih3wxnm0vkdnynm2ly43cr04qm917k83bj1gv71lnp";
            };
            nativeBuildInputs = [
                super.autoPatchelfHook
            ];
            dontConfigure = true;
            dontBuild = true;
            installPhase = ''
                mkdir -p $out
                mv ./* $out
            '';
        };
        
    })];
}