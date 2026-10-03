{
  description = "Development environment for saga";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      treefmt-nix,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        treefmtEval = treefmt-nix.lib.evalModule pkgs {
          projectRootFile = "flake.nix";
          settings.global.excludes = [
            "**/*.pdf"
            ".render/**"
          ];
          programs.gleam.enable = true;
          programs.nixfmt.enable = true;
          programs.prettier.enable = true;
        };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            lefthook
            gleam
            beam28Packages.erlang
            rebar3
            # A disposable PostgreSQL 16 for integrations/saga_postgres's
            # storage conformance run (scripts/test-postgres.sh).
            postgresql_16
          ];
        };

        # Elixir-equipped shell for the Reactor differential oracle
        # (oracle/reactor/). Kept out of devShells.default and CI so the
        # package's own toolchain stays Elixir-free; run explicitly with
        # `nix develop .#oracle -- scripts/oracle.sh`.
        devShells.oracle = pkgs.mkShell {
          packages = with pkgs; [
            lefthook
            gleam
            beam28Packages.erlang
            beam28Packages.elixir
            rebar3
          ];
        };

        formatter = treefmtEval.config.build.wrapper;

        checks.formatting = treefmtEval.config.build.check ./.;
      }
    );
}
