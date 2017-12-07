How to release
--------------

- edit the `version.sbt` file
- commit
- tag it as `vX.Y.Z`
- push things
- `sbt -Dpublish.maven.central=true publishSigned` and remember to publish for 2.12 as well
  - `++2.12.X` && `publishSigned`
- update the `akka-persistence-dynamodb-xx-stable` reporting project in [WhiteSource](http://saas.whitesourcesoftware.com/)
- `sbt whitesourceUpdate`
