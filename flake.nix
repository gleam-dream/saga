{
  description = "Development environment for saga";

  inputs = {
    design-layer.url = "github:lostbean/design-layer/main";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      design-layer,
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
            "**/.render/**"
            "**/build/**"
            "**/.artifacts/**"
            "oracle/**"
            "fixtures/**"
          ];
          programs.gleam.enable = true;
          programs.nixfmt.enable = true;
          programs.prettier.enable = true;
          programs.ruff.format = true;
          programs.shfmt.enable = true;
          settings.formatter.shfmt.options = [
            "-i"
            "2"
          ];
        };
      in
      {
        apps.snips = design-layer.apps.${system}.snips;
        apps.design-gate-check = design-layer.apps.${system}.check;
        apps.design-gate-render = design-layer.apps.${system}.render;
        apps.design-gate-context = design-layer.apps.${system}.context;

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            git
            lefthook
            gleam
            beam29Packages.erlang
            beam29Packages.rebar3
            python3
            ruff
            # A disposable PostgreSQL 16 for integrations/saga_postgres's
            # storage conformance run (scripts/test-postgres.sh).
            postgresql_16
          ];
        };

        # Elixir-equipped shell for the Reactor differential oracle
        # (oracle/reactor/). The ordinary shell stays Elixir-free; extended
        # CI and local oracle checks explicitly select this shell with
        # `nix develop .#oracle --command scripts/oracle.sh`.
        devShells.oracle = pkgs.mkShell {
          packages = with pkgs; [
            git
            lefthook
            gleam
            beam29Packages.erlang
            beam29Packages.elixir_1_20
            beam29Packages.rebar3
            python3
            ruff
          ];
        };

        formatter = treefmtEval.config.build.wrapper;

        checks.formatting = treefmtEval.config.build.check ./.;
      }
    );
}
