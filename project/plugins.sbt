resolvers += Resolver.jcenterRepo

addSbtPlugin("com.geirsson"                      % "sbt-scalafmt"        % "1.5.1")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"       % "1.5.1")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"    % "latest.release")
addSbtPlugin("de.heikoseeberger"                 % "sbt-header"          % "5.1.0")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-multi-jvm"       % "0.4.0")
addSbtPlugin("io.gatling"                        % "gatling-sbt"         % "3.0.0")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-native-packager" % "1.3.2")
addSbtPlugin("com.eltimn"                        % "sbt-frontend"        % "1.1.0")
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"        % "0.9.4")
