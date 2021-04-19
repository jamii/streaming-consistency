let 
  
  pkgs = import ../pkgs.nix;
  
in 

pkgs.mkShell {
    buildInputs = [
        pkgs.maven
        pkgs.jre8
        pkgs.flink
        
        pkgs.which
        pkgs.python38
    ];
}