let 
  
  pkgs = import ../pkgs.nix;
  
in 

pkgs.mkShell {
    buildInputs = [
        pkgs.maven
        pkgs.jre8
        pkgs.apacheKafka
        pkgs.spark
        
        pkgs.python38
        pkgs.which
    ];
}