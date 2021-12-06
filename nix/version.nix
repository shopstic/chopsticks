builtins.head (builtins.match ''ThisBuild / version := "([^"]+)"''\n'' (builtins.readFile ../version.sbt))
