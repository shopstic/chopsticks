{
  description = "Chopsticks";

  inputs = {
    hotPot.url = "github:shopstic/nix-hot-pot";
    nixpkgs.follows = "hotPot/nixpkgs";
    flakeUtils.follows = "hotPot/flakeUtils";
    fdb.follows = "hotPot/fdb";
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
                  jdk = hotPotPkgs.jdk17;
                };
              })
            ];
          };
          fdbLibSystem = if system == "aarch64-darwin" then "x86_64-darwin" else system;
          hotPotPkgs = hotPot.packages.${system};
          chopsticksSystem = if system == "aarch64-linux" then "x86_64-linux" else system;
          chopsticksPkgs = import nixpkgs { system = chopsticksSystem; };

          jdkArgs = [
            "--set DYLD_LIBRARY_PATH ${fdb.defaultPackage.${fdbLibSystem}}/lib"
            "--set LD_LIBRARY_PATH ${fdb.defaultPackage.${fdbLibSystem}}/lib"
          ];

          runJdk = pkgs.callPackage hotPot.lib.wrapJdk {
            jdk = hotPot.packages."${fdbLibSystem}".jdk17;
            args = pkgs.lib.concatStringsSep " " jdkArgs;
          };
          compileJdk = pkgs.callPackage hotPot.lib.wrapJdk {
            jdk = hotPotPkgs.jdk17;
            args = pkgs.lib.concatStringsSep " " (jdkArgs ++ [''--run "if [[ -f ./.env ]]; then source ./.env; fi"'']);
          };
          sbt = pkgs.sbt.override {
            jre = {
              home = compileJdk;
            };
          };
          jdkPrefix = "chopsticks-";
          updateIntellij = pkgs.writeShellScript "update-intellij" ''
            set -euo pipefail

            THIS_PATH=$(realpath .)
            SDK_NAMES=(compile run)

            for SDK_NAME in "''${SDK_NAMES[@]}"
            do
              find ~/Library/Application\ Support/JetBrains/ -mindepth 1 -maxdepth 1 -name "IntelliJIdea*" -type d | \
                xargs -I%%%% bash -c "echo \"Adding ${jdkPrefix}''${SDK_NAME} to %%%%/options/jdk.table.xml\" && ${hotPotPkgs.intellij-helper}/bin/intellij-helper \
                update-jdk-table-xml \
                --name ${jdkPrefix}''${SDK_NAME} \
                --jdkPath \"''${THIS_PATH}\"/.dev-sdks/\"''${SDK_NAME}\"-jdk \
                --jdkTableXmlPath \"%%%%/options/jdk.table.xml\" \
                --inPlace=true"
            done
          '';
          devSdks = pkgs.linkFarm "dev-sdks" [
            { name = "compile-jdk"; path = compileJdk; }
            { name = "run-jdk"; path = runJdk; }
            { name = "update-intellij"; path = updateIntellij; }
          ];

          chopsticksDeps = chopsticksPkgs.callPackage ./nix/deps.nix {
            inherit sbt;
            jdk = hotPotPkgs.jdk17;
          };

          chopsticks = chopsticksPkgs.callPackage ./nix/chopsticks.nix {
            inherit sbt;
            jdk = hotPotPkgs.jdk17;
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
                inherit compileJdk sbt;
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
