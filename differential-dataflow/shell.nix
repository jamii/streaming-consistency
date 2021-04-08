
let 
  
  pkgs = import ../pkgs.nix;
  
in 

pkgs.mkShell {
    buildInputs = [
        pkgs.rustup
        
        pkgs.python38
    ];
}