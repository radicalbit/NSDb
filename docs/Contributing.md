# Contributing guidelines

In the [GitHub issue Page](https://github.com/radicalbit/nsdb/issues), you can find a list of tasks to be implemented or bugs already discovered.

When you find a new bug, you can open a new issue making sure that:
- a brief description and a path to replicate it is provided.

If you want to propose a new feature or if you are not sure that what you need is a bug or a feature:
- Write an email about your idea to nsdb@radicalbit.io or ask for feedback to the project gitter channel. We will help you to analyze your issue and discuss together how to proceed.

Before submitting a pull request (PR), please follow these guidelines:
* A PR must be always associated with a task or a bug, thus make sure that title starts with the issue code and that the issues is linked in the description.
* Make sure to follow this naming convention when opening a new branch for a PR:
  - `bugfix/[PR Code]` for a bug.
  - `feature/[PR Code]` for a new feature.
* Execute tests locally by running `sbt clean test`. This guarantees, besides tests are executed successfully that:
  - code is properly formatted (by scalafmt plugin).
  - license headers are generated on each java or scala file created.
* Make sure to add or update documentation according to the changes that are being submitted.
* Make suer to add all the necessary tests that cover the bug or the new feature provided in the PR.
* Do not use author tags in the code.
* PR must not have conflict with master branch.
