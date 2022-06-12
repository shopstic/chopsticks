{
  description = "Chopsticks";

  inputs = {
    hotPot.url = "github:shopstic/nix-hot-pot";
    nixpkgs.follows = "hotPot/nixpkgs";
    flakeUtils.follows = "hotPot/flakeUtils";
    fdb.url = "github:shopstic/nix-fdb/7.1.9";
  };

  outputs = { self, nixpkgs, flakeUtils, hotPot, fdb }:
    flakeUtils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              (final: prev: {
                maven = prev.maven.override {
                  jdk = prev.jdk11;
                };
              })
            ];
          };
          hotPotPkgs = hotPot.packages.${system};
          chopsticksSystem = if system == "aarch64-linux" then "x86_64-linux" else system;
          chopsticksPkgs = import nixpkgs { system = chopsticksSystem; };

          fdbLib = fdb.packages.${system}.fdb_7.lib;
          jdkArgs = [
            "--set DYLD_LIBRARY_PATH ${fdbLib}"
            "--set LD_LIBRARY_PATH ${fdbLib}"
            "--set JDK_JAVA_OPTIONS -DFDB_LIBRARY_PATH_FDB_JAVA=${fdbLib}/libfdb_java.${if pkgs.stdenv.isDarwin then "jnilib" else "so"}"
          ];
          jdk = pkgs.callPackage hotPot.lib.wrapJdk {
            jdk = pkgs.jdk11;
            args = pkgs.lib.concatStringsSep " " (jdkArgs ++ [''--run "if [[ -f ./.env ]]; then source ./.env; fi"'']);
          };
          sbt = pkgs.sbt.override {
            jre = {
              home = jdk;
            };
          };
          jdkPrefix = "chopsticks-";
          updateIntellij = pkgs.writeShellScript "update-intellij" ''
            set -euo pipefail

            THIS_PATH=$(realpath .)
            find ~/Library/Application\ Support/JetBrains/ -mindepth 1 -maxdepth 1 -name "IntelliJIdea*" -type d | \
                xargs -I%%%% bash -c "echo \"Adding ${jdkPrefix}jdk to %%%%/options/jdk.table.xml\" && ${hotPotPkgs.intellij-helper}/bin/intellij-helper \
                update-jdk-table-xml \
                --name ${jdkPrefix}jdk \
                --jdkPath \"''${THIS_PATH}\"/.dev-sdks/jdk \
                --jdkTableXmlPath \"%%%%/options/jdk.table.xml\" \
                --inPlace=true"
          '';
          devSdks = pkgs.linkFarm "dev-sdks" [
            { name = "jdk"; path = jdk; }
            { name = "update-intellij"; path = updateIntellij; }
          ];

          chopsticksDeps = chopsticksPkgs.callPackage ./nix/deps.nix {
            inherit sbt;
            jdk = pkgs.jdk11;
          };

          chopsticks = chopsticksPkgs.callPackage ./nix/chopsticks.nix {
            inherit sbt;
            jdk = pkgs.jdk11;
            inherit chopsticksDeps;
          };
        in
        rec {
          defaultPackage = chopsticks;
          packages = {
            coursierCache = chopsticksDeps;
          };
          devShell = pkgs.mkShellNoCC rec {
            shellHook = ''
              ln -Tfs ${devSdks} ./.dev-sdks
            '';
            buildInputs = builtins.attrValues
              {
                inherit jdk sbt;
                inherit (pkgs)
                  jq
                  parallel
                  maven
                  gnupg
                  ;
              };
          };
        }
      );
}
