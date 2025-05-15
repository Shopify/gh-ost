## Development setup for `gh-ost` at Shopify

Requirements:
- podman, podman machine
- docker client

Running go tests

```
dev test
```

Running `localtests` (integration test suite)

```
dev localtests
```

Run a single localtest
```
script/build

export PATH="${PATH}:$(pwd)/script"
./localtests/test.sh -b bin/gh-ost -d swap-uk-uk
```

## Git workflow

The following branched workflow ensures Shopify can contribute to the upstream repo, while maintaining a stable release channel and the ability to quickly iterate on features and patches to upstream `gh-ost`.

`master`: tracking `github/gh-ost@master`, our release branch where we merge new features and where we cut Shopify releases from using tags following a versioning scheme like `v1.1.7-shopify-1234567`, where the suffix is a short commit SHA. If there are changes in `github/gh-ost@master` that we synced but would not want release temporarily (e.g. until an official tagged release), we can create a new release branch to select the desired commits.

`username/feature-name`: feature branches where we open PRs both to `shopify/gh-ost@master` and `github/gh-ost@master`

Generally, Shopify-specific fixes should be avoided in favor of following the contributing guidelines of `github/gh-ost` and making sure all changes can be merged upstream. This ensures the fork won't diverge and the Shopify-specific build becomes the only long-term option to use it. Don't forget to open a GitHub issue (proposal) in `github/gh-ost` before opening upstream pull requests.

### Releasing gh-ost

1. `git tag v1.1.7-shopify-$(git rev-parse HEAD | cut -c1-7)`
2. `git push your-shopify-remote --tags`
3. A GitHub Actions workflow will build and release automatically.
4. Go to the [releases](https://github.com/Shopify/gh-ost/releases) page, select your release and click the pencil icon and “Generate Release Notes”. This will automatically describe all PRs included, but may not include changes pulled from `master`, which you need to document manually.
5. Grab the `.deb` file from the [releases](https://github.com/Shopify/gh-ost/releases) page. Find the corresponding checksum in the GitHub Action build logs.
