# Releasing

## dapr-durabletask-proto

There are automated workflows to automatically update and tag new rust-protos of the tagged upstream dapr/durabletask-protobuf definitions.

Changes are added to the main branch and may need to be backported, this is not done automatically.

## dapr-durabletask

The dapr-durabletask crate does not have automated version bumps. You must update the crate version manually and then tag a release (dapr-durabletask-v*)
which will trigger the release workflow.