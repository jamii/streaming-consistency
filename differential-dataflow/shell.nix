let 
  
  pkgs = import ../pkgs.nix;
  
in 

pkgs.mkShell {
    buildInputs = [
        pkgs.cargo
        
        pkgs.python38
    ];
}