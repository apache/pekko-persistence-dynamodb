How to release
--------------

- edit the `version.sbt` file
- commit
- tag it as `vX.Y.Z`
- push things
- `sbt publishSigned` and remember to publish for 2.12 as well
  - `++2.12.X` && `publishSigned`
