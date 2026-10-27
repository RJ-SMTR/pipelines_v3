# Dataproc Python image

The workflow `.github/workflows/build-and-push-dataproc-image.yaml` publishes
`us-central1-docker.pkg.dev/rj-smtr/dataproc/dbt-python:latest`, used by the dbt
profiles. It runs daily at 06:23 UTC (03:23 in São Paulo), on changes to the image
inputs merged into `master`, and through **Run workflow**.

The requirements contain package names without version constraints. Every build
installs the latest compatible releases from PyPI with `pip install --upgrade`.
Dependency constraints declared by those packages still apply. The workflow
checks PyPI's `rio-rac-bus-subsidy` metadata to detect new releases and verifies
that the installed package matches that version before publishing. An incompatible
release, a version change during the build or an incomplete publication fails
the check and leaves the current image unchanged. TestPyPI is not considered.
Manual runs use the same latest-version behavior.

The workflow fingerprints the package version, Dockerfile, Docker ignore file,
requirements and workflow. If the corresponding `build-<hash>` tag exists, it skips the build.
Otherwise it builds for `linux/amd64`, checks dependencies with `pip check`,
checks the package version and imports used by the dbt Python model, and pushes
the image. Only verified images are tagged `latest`. Existing verified images
are reused and promoted to `latest`. Docker layer caching is disabled for builds
so a new package release cannot reuse an old dependency installation.

Use `force` to rebuild the same inputs, for example to refresh the base image or
unbounded dependencies. Fingerprinted tags identify build inputs, not immutable
image contents when `force` is used. The workflow does not execute Spark or
validate the remuneration calculation.

The existing `GKE_SA_KEY` secret authenticates the build. Its service account
needs Artifact Registry write access to the existing `dataproc` repository in
`rj-smtr`. This workflow does not provision the repository or grant IAM access.
Scheduled runs start once the workflow reaches the default branch. There is no
cross-repository token or required change to the PyPI publishing workflow.

The build context contains only `requirements-dataproc.txt`; repository code
and credentials are not copied into the image. Standalone Docker builds also
install the latest compatible releases, without build arguments or substitutions.
Use `docker build --no-cache` when rebuilding locally to refresh dependencies.
