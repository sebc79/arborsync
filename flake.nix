{
  description = "ArborSync selective subtree file synchronization daemon";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      inherit (nixpkgs) lib;
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      linuxSystems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = lib.genAttrs systems;
      pkgsFor =
        system:
        import nixpkgs {
          inherit system;
          overlays = [ self.overlays.default ];
        };
    in
    {
      overlays.default = final: _prev: {
        arborsync = final.rustPlatform.buildRustPackage {
          pname = "arborsync";
          version = "0.1.0";
          src = final.lib.cleanSource ./.;
          cargoLock.lockFile = ./Cargo.lock;
          doCheck = false;
          meta = {
            description = "Selective subtree file synchronization daemon";
            license = with final.lib.licenses; [
              mit
              asl20
            ];
            mainProgram = "arborsync";
          };
        };
      };

      packages = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
        in
        {
          inherit (pkgs) arborsync;
          default = pkgs.arborsync;
        }
      );

      nixosModules.default =
        { pkgs, ... }:
        {
          imports = [ ./nix/module.nix ];
          services.arborsync.master.package = lib.mkDefault self.packages.${pkgs.stdenv.hostPlatform.system}.arborsync;
        };
      nixosModules.arborsync = self.nixosModules.default;

      checks = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
          linuxChecks = lib.optionalAttrs (lib.elem system linuxSystems) {
            module-unit =
              let
                eval = nixpkgs.lib.nixosSystem {
                  inherit system;
                  modules = [
                    self.nixosModules.default
                    {
                      boot.isContainer = true;
                      networking.hostName = "arborsync-check";
                      system.stateVersion = "25.05";
                      services.arborsync.master.enable = true;
                      services.arborsync.master.configFile = ./nix/test-master.toml;
                    }
                  ];
                };
                unit = eval.config.systemd.services.arborsync-master.serviceConfig;
                start = "${unit.ExecStart}";
                pre = "${unit.ExecStartPre}";
              in
              assert lib.hasInfix "arborsync master --config /run/arborsync/master.toml" start
                || throw "ExecStart missing runtime --config: ${start}";
              assert !(lib.hasInfix "--config /nix/store" start)
                || throw "ExecStart --config must not be a store path: ${start}";
              assert lib.hasInfix "install -m 0600" pre
                || throw "ExecStartPre missing install -m 0600: ${pre}";
              assert lib.hasInfix "/run/arborsync/master.toml" pre
                || throw "ExecStartPre missing runtime config path: ${pre}";
              pkgs.runCommand "arborsync-module-unit" { } "touch $out";
          };
        in
        linuxChecks
      );
    };
}
