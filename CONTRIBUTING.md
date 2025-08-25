# Contributing to otel-kafka

Contributions to otel-kafka of any kind are very welcome! These include documentation, tutorials, blog posts, bug reports, issues, feature requests,
feature implementations, pull requests, helping to manage issues, etc.

*Changes to the codebase **and** related documentation, e.g. for a new feature, should still use a single pull request.*

## Table of Contents

* [Reporting Issues](#reporting-issues)
* [Submitting Patches](#submitting-patches)
    * [Git Commit Message Guidelines](#git-commit-message-guidelines)
    * [Fetching the Sources From GitHub](#fetching-the-sources-from-github)

## Reporting Issues

If you believe you have found an issue in otel-kafka or its documentation, use
the GitHub issue tracker to report the problem to the maintainers.

- [otel-kafka Issues · ekazakas/otel-kafka](https://github.com/ekazakas/otel-kafka/issues)

## Submitting Patches

The otel-kafka project welcomes all contributors and contributions regardless of skill or experience level. If you are interested in helping with the project, we will help you with your contribution.

### Code Contribution Guidelines

Because we want to create the best possible product for our users and the best contribution experience for our developers, we have a set of guidelines which ensure that all contributions are acceptable. The guidelines are not intended as a filter or barrier to participation. If you are unfamiliar with the contribution process, the otel-kafka maintainers will help you and teach you how to bring your contribution in accordance with the guidelines.

To make the contribution process as seamless as possible, we ask for the following:

* Go ahead and fork the project and make your changes.  We encourage pull requests to allow for review and discussion of code changes.
* When you’re ready to create a pull request, be sure to:
    * Have test cases for the new code. If you have questions about how to do this, please ask in your pull request.
    * Run `go fmt`.
    * Add documentation if you are adding new features or changing functionality.
    * Squash your commits into a single commit. `git rebase -i`. It’s okay to force update your pull request with `git push -f`.
    * Ensure that `go test` succeeds.
    * Follow the **Git Commit Message Guidelines** below.

### Git Commit Message Guidelines

This [blog article](https://cbea.ms/git-commit/) is a good resource for learning how to write good commit messages,
the most important part being that each commit message should have a title/subject in imperative mood starting with a capital letter and no trailing period:
*"js: Return error when option x is not set"*, **NOT** *"returning some error."*

Most title/subjects should have a lower-cased prefix with a colon and one whitespace. The prefix can be:

* The name of the package where (most of) the changes are made (e.g. `media: Add text/calendar`)
* If the package name is deeply nested/long, try to shorten it from the left side, e.g. `markup/goldmark` is OK, `resources/resource_transformers/js` can be shortened to `js`.
* If this commit touches several packages with a common functional topic, use that as a prefix, e.g. `errors: Resolve correct line numbers`)
* If this commit touches many packages without a common functional topic, prefix with `all:` (e.g. `all: Reformat Go code`)
* If this is a documentation update, prefix with `docs:`.
* If nothing of the above applies, just leave the prefix out.
* Note that the above excludes nouns seen in other repositories, e.g. "chore:".

Also, if your commit references one or more GitHub issues, always end your commit message body with *See #1234* or *Fixes #1234*.
Replace *1234* with the GitHub issue ID. The last example will close the issue when the commit is merged into *master*.

An example:

```text
tpl: Add custom index function

Add a custom index template function that deviates from the stdlib simply by not
returning an "index out of range" error if an array, slice or string index is
out of range.  Instead, we just return nil values.  This should help make the
new default function more useful for otel-kafka users.

Fixes #1949
```

###  Fetching the Sources From GitHub

Since otel-kafka uses the Go Modules support built into Go 1.11 to build. The easiest is to clone otel-kafka in a directory outside of `GOPATH`, as in the following example:

```bash
mkdir $HOME/src
cd $HOME/src
git clone https://github.com/ekazakas/otel-kafka.git
cd otel-kafka
go build ./...
```

Now, to make a change to otel-kafka source:

1. Create a new branch for your changes (the branch name is arbitrary):

    ```bash
    git checkout -b iss1234
    ```

2. After making your changes, commit them to your new branch:

    ```bash
    git commit -a -v
    ```

3. Fork otel-kafka in GitHub.

4. Add your fork as a new remote (the remote name, "fork" in this example, is arbitrary):

    ```bash
    git remote add fork git@github.com:USERNAME/otel-kafka.git
    ```

5. Push the changes to your new remote:

    ```bash
    git push --set-upstream fork iss1234
    ```

6. You're now ready to submit a PR based upon the new branch in your forked repository.
