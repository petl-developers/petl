
This PR has the objective of <describe it here>.

## Changes

1. Added new feature for...
2. Fixed a bug in...
3. Changed the behavior of...
4. Improved the docs about...

## Checklist

Use this checklist to ensure the quality of pull requests that include new code and/or make changes to existing code.

* [ ] Source Code guidelines:
  * [ ] Includes unit tests
  * [ ] New functions have docstrings with examples that can be run with doctest
  * [ ] New functions are included in API docs
  * [ ] Docstrings include notes for any changes to API or behavior
  * [ ] All changes are documented in docs/changes.rst
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
* [ ] State of these changes is:
  * [ ] Just a proof of concept
  * [ ] Work in progress / Further changes needed
  * [ ] Ready to review
  * [ ] Ready to merge
