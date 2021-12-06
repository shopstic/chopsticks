{ lib
, stdenv
, jdk
, sbt
, chopsticksDeps
}:
stdenv.mkDerivation {
  pname = "chopsticks";
  version = import ./version.nix;

  src = builtins.path
    {
      path = ../.;
      name = "src";
      filter = (path: /* type */_:
        lib.hasInfix "/chopsticks-" path ||
        lib.hasInfix "/project" path ||
        lib.hasSuffix ".sbt" path ||
        lib.hasSuffix ".scalafmt.conf" path
      );
    };

  buildInputs = [
    jdk
    sbt
  ];

  configurePhase = ''
    cp -R "${chopsticksDeps}/cache" "$TMPDIR/"

    export XDG_CACHE_HOME="$TMPDIR/cache"
    chmod -R +w "$XDG_CACHE_HOME"

    export PROTOC_CACHE="$XDG_CACHE_HOME/protoc_cache";
    export COURSIER_CACHE="$XDG_CACHE_HOME/coursier";

    export SBT_OPTS="-Dsbt.global.base=$XDG_CACHE_HOME/sbt -Dsbt.ivy.home=$XDG_CACHE_HOME/ivy -Xmx4g -Xss6m"
    echo "SBT_OPTS=$SBT_OPTS"

    sbt --client cq < <(echo q)
  '';

  buildPhase = ''
    sbt --client compile
    sbt --client Test / compile
  '';

  installPhase = ''
    sbt --client 'set ThisBuild / Test / publishArtifact := true'
    # sbt --client 'set ThisBuild / publishMavenStyle := false'
    sbt --client "set ThisBuild / publishTo := Some(Resolver.file(\"local-maven\", file(\"$out\")))"
    sbt --client publish
    sbt --client shutdown
  '';

  doCheck = true;
  dontStrip = true;
  dontPatch = true;
  dontFixup = true;

  meta = with lib;
    {
      description = "Chopsticks";
      homepage = "https://chopsticks.dev";
      license = licenses.asl20;
      platforms = [ "x86_64-darwin" "aarch64-darwin" "x86_64-linux" ];
    };
}
