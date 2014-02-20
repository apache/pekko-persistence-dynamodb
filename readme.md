### setup

* run `bin/get-dynamodb-local`

this downloads and unpacks the dynamodb local to a subdir of ./dynamodb-local

* `cp .env.sample .env`

* make sure the DYNAMODB_RELEASE var in .env matches the date of the distro that was placed in ./dynamodb-local

* `forego start`

This starts the local dynamodb instance

In another shell

* forego run sbt test