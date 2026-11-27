
This changes <describe what you are changing> because <describe what you want to accomplish>.

## Changes

1. Added a new feature for...
2. Fixed a bug in...
3. Changed the behavior of...
4. Improved the docs about...

## Checklist

Use this checklist to ensure the quality of the source code modified by the pull request:

* [ ] Source Code guidelines:
  * [ ] Includes unit tests
  * [ ] New functions have docstrings with examples that can be run with doctest
  * [ ] New functions are included in API docs
  * [ ] Docstrings include notes for any changes to API or behavior
* [ ] Versioning and history tracking guidelines:
  * [ ] Using atomic commits whenever possible
  * [ ] Commits are reversible whenever possible
  * [ ] There are no incomplete changes in the pull request
  * [ ] There is no accidental garbage added to the source code
* [ ] Testing guidelines:
  * [ ] Tested locally using `tox` / `pytest`
  * [ ] Rebased to `master` branch and tested before sending the PR
  * [ ] Automated testing passes (see [CI](https://github.com/petl-developers/petl/actions))
  * [ ] Unit test coverage has not decreased (see [Coveralls](https://coveralls.io/github/petl-developers/petl))
  * [ ] Fixed the issues found by the Agentic code review

## Conventions

* Keep the PR as draft while developing and request a review when ready.
* Keep the pull request description and the commit messages concise:
  * Don't add text explaining the changes that could be inferred from the commit diff.
  * Don't explain the source code but tell the reasons, impacts, and objectives of the change.
  * You may omit saying that local tests passed, as this is already a requirement and is redundant.
