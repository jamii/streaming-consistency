let 
  
  pkgs = import ../pkgs.nix;
  
in 

pkgs.mkShell {
    buildInputs = [
        pkgs.materialized
        
        pkgs.python38
        pkgs.python38Packages.psycopg2
        pkgs.postgresql # provides psql
        pkgs.expect # provides unbuffer
    ];
}