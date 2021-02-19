let 
  
  pkgs = import ./pkgs.nix;
  
  apacheKafka = pkgs.apacheKafka.overrideAttrs (oldAttrs: rec {
      version = "2.7.0";
      src = pkgs.fetchurl {
          url = "mirror://apache/kafka/${version}/kafka_2.13-${version}.tgz";
          sha256 = "143zrghrq40lrwacwyzqkyzg4asax7kxg9cgnkn2z83n6rv4pn0x";
      };
      installPhase = ''
        mkdir -p $out/bin
        cp bin/zookeeper* $out/bin
      '' + oldAttrs.installPhase;
  });
  
in 

pkgs.mkShell {
    buildInputs = [
        apacheKafka
        pkgs.maven
        pkgs.jre8
    ];
}