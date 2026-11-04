# Contributing to RawWise

RawWise is a proprietary, source-available project. External pull requests are
welcome, but **all contributions require a signed CLA**, and **all rights in
accepted contributions belong to the Owner** — the individual operating the
`@rawwise` GitHub account (see [CLA.md](CLA.md)).

## Before You Contribute

1. Read [CLA.md](CLA.md).
2. Open an issue to discuss significant changes before writing a large patch.
3. Do not submit code you did not write, or code that carries a third-party
   license incompatible with a proprietary project.

## Sign the CLA

Pull requests cannot be merged until the CLA is signed. To sign, comment on your
pull request:

```
I have read the CLA Document and I hereby sign the CLA.

Name:
Email:
GitHub username:
```

By signing, you assign to the Owner (the individual operating `@rawwise`) all
copyright and related rights in your contribution, as described in
[CLA.md](CLA.md). The signature covers **all of your contributions, past and
future**, and you retain no right to use the Project.

Submitting a pull request, patch, or attachment without signing does not grant
any rights to the Project; such a pull request will not be reviewed or merged.

### Contributing on behalf of an employer

If you write the contribution as part of your job, with employer equipment or
time, or on behalf of a company, the rights usually belong to that company, not
to you. In that case either your employer must sign the Project's Corporate CLA
(template available on request from the Owner), or you must obtain a written
waiver from it before opening the pull request. Individual signatures alone are
not sufficient in this situation.

Use the same name and email that appear in your commits. If you commit with a
GitHub `noreply` address, sign with that same address so the signature can be
matched to your commits.

Pull requests without a signed CLA will not be reviewed or merged.

## Development Environment

| Requirement | Version |
|---|---|
| JDK | 21 |
| Maven | 3.9+ |
| OS | Linux / macOS |

Build and format:

```bash
# Compile and package without tests
mvn -B -DskipTests package

# Apply the project code style (Google Java Format, AOSP)
mvn -B spotless:apply
```

Run locally after packaging:

```bash
bash bin/rawwise-start.sh
bash bin/rawwise-status.sh
bash bin/rawwise-stop.sh
```

Notes:

- `bin/` only ships the local lifecycle scripts. Cluster/deployment
  configuration is intentionally not part of this repository.
- Most integration tests require a large heap and native libraries (LMDB,
  RocksDB). Some heavy suites are excluded from the default Surefire run; keep
  the excluded list in `rawwise-core/pom.xml` unchanged unless the test is
  proven stable.
- Never commit credentials, private endpoints, or customer data. Use
  `.env.example` as the template for local configuration.

## Pull Request Checklist

- [ ] CLA signed in a PR comment
- [ ] Change is focused and described in the PR body
- [ ] `mvn -B -DskipTests package` passes locally
- [ ] `mvn -B spotless:apply` applied (no formatting drift)
- [ ] No secrets, tokens, internal hosts, or absolute local paths added
- [ ] New dependencies justified in the PR description
- [ ] Documentation updated if behavior changes

## Code Style

- Google Java Format (AOSP style), enforced by Spotless
- No new license headers; the project is proprietary (`COPYRIGHT`)
- Avoid adding new third-party dependencies unless clearly necessary

## Security Issues

Do not open a public issue for security problems. See [SECURITY.md](SECURITY.md).

## Licensing of Accepted Contributions

Accepted contributions become the property of the Owner (the individual
operating `@rawwise`) and are covered by `COPYRIGHT`: all rights reserved, no
license granted to any third party. The project may be licensed, sublicensed,
or relicensed at the Owner's sole discretion.
