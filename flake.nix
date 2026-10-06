{
  description = "Development environment for saga";

  inputs = {
    design-layer.url = "github:lostbean/design-layer";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
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

        # The upstream apps pin the renderer; authored imports also need its
        # generated local projection on a fresh checkout.
        designApp =
          name:
          let
            wrapper = pkgs.writeShellApplication {
              name = "design-gate-${name}";
              runtimeInputs = [ pkgs.coreutils ];
              text = ''
                project_layer() {
                  if [ -f "$1/design.typ" ]; then
                    mkdir -p "$1/.render"
                    cp -RL --remove-destination --no-preserve=mode ${
                      design-layer.packages.${system}.gate-bundle
                    }/render/. "$1/.render/"
                  fi
                }
                project_layer "''${1:-docs/design}"
                ${pkgs.lib.optionalString (name == "check") ''
                  project_layer "''${2:-.}/integrations/saga_postgres/docs/design"
                ''}
                exec ${design-layer.apps.${system}.${name}.program} "$@"
              '';
            };
          in
          {
            type = "app";
            program = "${wrapper}/bin/design-gate-${name}";
          };

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
        apps.design-gate-check = designApp "check";
        apps.design-gate-render = designApp "render";
        apps.design-gate-context = designApp "context";

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
