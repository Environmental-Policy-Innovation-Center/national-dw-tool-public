---
title: "Contributing-Guidelines"
author: "EmmaLi Tsai"
date: "2026-05-13"
output: 
  html_document:
    keep_md: true
---

# Contributing Guidelines

*These guidelines were inspired by those implemented by the [Public Environmental Data Partners](https://github.com/Public-Environmental-Data-Partners/overview/blob/main/CONTRIBUTING.md)*

We're thrilled you're interested in contributing to our Drinking Water Dataset & Explorer tool! There are two main ways to contribute:

- [Feedback, feature requests, and bug reports](#feedback-requests-and-bug-reports)
- [Code and documentation changes](#code-and-documentation-changes)

## Feedback, requests, and bug reports {#feedback-requests-and-bug-reports}

For submitting feedback, feature requests, or bug reports, you can begin the conversation by creating an issue on our GitHub repositories. For anything related to data, please submit issues in the [national-dw-tool-public](https://github.com/Environmental-Policy-Innovation-Center/national-dw-tool-public) repository, for anything related to the tool (i.e., bugs, typos, etc.) please submit issues in the [water-data-tool](https://github.com/Environmental-Policy-Innovation-Center/water-data-tool) repository. Please utilize the default tags & templates to help us better understand your comment.

For issues submitted in Github, codebase maintainers aim to review and respond to the submitted issue within a week.

## Code and documentation changes {#code-and-documentation-changes}

We love contributions, whether code or documentation (or both!). If you're interested in seeing what we're currently working on, each repository has a project board (see the [data](https://github.com/orgs/Environmental-Policy-Innovation-Center/projects/4) & [tool](https://github.com/orgs/Environmental-Policy-Innovation-Center/projects/5) project boards). Please note that a single issue may appear in both the data and tool project boards where there is overlapping needs or requests. Tickets with the "good first issue" label are ones that would be great for an external contributor to pick up!

Our process for accepting changes operates by [Pull Request (PR)](https://help.github.com/articles/about-pull-requests/) and has a few steps:

1.  If you haven't submitted anything before, and you aren't a member of our organization, fork and clone the repo:

    \$ git clone [git\@github.com](mailto:git@github.com){.email}:<your-username>/<repository-name>.git

2.  Create a new branch for the changes you want to work on. Choose a branch name that reflects the subject of the change, e.g. `map-layers` for a branch tackling adding layers to a map.

    git checkout -b <branch-name>

3.  Create or modify the files with your changes and commit them in Git. If you are fixing a known issue, include “fixes #123” (where “123” is the issue number) in one of your commit messages to help automatically link everything together.

4.  Once your changes are ready for review, push your commits to GitHub and create a pull request (PR) to **our `dev` branch**. We generally try to keep all development code on a separate branch from `main`. If you aren’t ready for final review and just need some preliminary feedback, create the PR as a draft, and then move it out of Draft status when you're ready for review.

5.  Allow others sufficient time for review and comments before merging. We make use of GitHub's review feature to comment in-line on PRs when possible. There may be some fixes or adjustments you'll have to make based on feedback.

In general, we do our best to provide some feedback or review within \~3 business days!

When merging PRs, we generally use the “squash and merge” feature on GitHub so that each PR has a single, clear commit, and so that the commit title links back to the PR by its number. This isn’t a hard rule. If you have a specific reason to do an actual merge or a rebase, then do so.

Finally, as an organization maintaining open-source projects, we value and respect each community member. However, contributing doesn't guarantee that new changes will be merged or that feedback or requests will be prioritized. Factors that influence this decision include, but are not limited to, the contribution's quality, alignment with the project's goals or roadmap (which may change), impact on the codebase's stability, and the resources required to implement or support it.

We understand that putting time and effort into a contribution only for it to be rejected can be disheartening. However, the aim is always to ensure that our projects remain high-quality, maintainable, and focused on meeting the project's and its users' needs. Please do not take the rejection of a specific contribution as a rejection of your participation in the project!

Thank you again for your interest in contributing - don't hesitate to reach out if you have any questions!
