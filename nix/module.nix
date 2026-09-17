{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.arborsync.master;
  runtimeConfig = "/run/arborsync/master.toml";
  installConfig = "${pkgs.coreutils}/bin/install -m 0600 ${cfg.configFile} ${runtimeConfig}";
in
{
  options.services.arborsync.master = {
    enable = lib.mkEnableOption "the ArborSync master daemon";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.arborsync;
      defaultText = lib.literalExpression "pkgs.arborsync";
      description = "Package that provides the arborsync binary.";
    };

    configFile = lib.mkOption {
      type = lib.types.path;
      description = ''
        Master TOML. The unit copies this file to ${runtimeConfig} with
        mode 0600 before start and on reload.
      '';
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "arborsync";
      description = "User the master process runs as.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "arborsync";
      description = "Group the master process runs as.";
    };
  };

  config = lib.mkIf cfg.enable {
    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.group;
      home = "/var/lib/arborsync";
    };

    users.groups.${cfg.group} = { };

    systemd.services.arborsync-master = {
      description = "ArborSync master";
      after = [ "network.target" ];
      wantedBy = [ "multi-user.target" ];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        RuntimeDirectory = "arborsync";
        RuntimeDirectoryMode = "0750";
        StateDirectory = "arborsync";
        ExecStartPre = installConfig;
        ExecStart = "${lib.getExe cfg.package} master --config ${runtimeConfig}";
        ExecReload = [
          installConfig
          "${pkgs.coreutils}/bin/kill -HUP $MAINPID"
        ];
        Restart = "on-failure";
      };
    };
  };
}
