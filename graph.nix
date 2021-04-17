let 
  
  pkgs = import ./pkgs.nix;
  
in 

pkgs.mkShell {
    buildInputs = [
        (pkgs.rWrapper.override{packages = [
             pkgs.rPackages.data_table 
             pkgs.rPackages.ggplot2
        ];})
    ];
}


