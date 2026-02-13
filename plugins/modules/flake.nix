{
  description = "ansible flake";

  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.2511.0";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        # Python dependencies
        python_env = pkgs.python312.withPackages (
          ps: with ps; [
            black
            ansible-core
            requests
            pymupdf
          ]
        );
      in
      {
        devShell = pkgs.mkShell {
          buildInputs = [
            python_env
          ];
          shellHook = ''
            export ANSIBLE_COLLECTIONS_PATH="$PWD/.ansible/collections"
            mkdir -p $ANSIBLE_COLLECTIONS_PATH
            # Enable Node.js performance optimizations
            export NODE_OPTIONS="--max-old-space-size=4096 --experimental-worker"
            export UV_THREADPOOL_SIZE=8  # Adjust based on your CPU cores
            export PYTHONPATH="${python_env}/${python_env.sitePackages}";
          '';
        };
      }
    );
}

