let 
  
  pkgs = import ../pkgs.nix;
  
in 

pkgs.mkShell {
    buildInputs = [
        pkgs.which
        pkgs.python38
    ];
}