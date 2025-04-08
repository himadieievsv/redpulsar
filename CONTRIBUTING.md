# Contributing to RedPulsar

Thank you for your interest in contributing to RedPulsar! Your contributions help improve and evolve this project. Please follow the guidelines below to ensure a smooth collaboration.

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [How Can I Contribute?](#how-can-i-contribute)
    - [Reporting Bugs](#reporting-bugs)
    - [Suggesting Enhancements](#suggesting-enhancements)
    - [Submitting Pull Requests](#submitting-pull-requests)
3. [Development Guidelines](#development-guidelines)
    - [Setting Up the Development Environment](#setting-up-the-development-environment)
    - [Coding Standards](#coding-standards)
    - [Testing](#testing)
4. [License](#license)

## Code of Conduct

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project, you agree to abide by its terms.

## How Can I Contribute?

### Reporting Bugs

If you encounter a bug, please create an issue on GitHub with the following information:

- **Description**: A clear and concise description of the bug.
- **Steps to Reproduce**: Steps to reproduce the behavior.
- **Expected Behavior**: What you expected to happen.
- **Actual Behavior**: What actually happened.
- **Environment**: Information about your environment (e.g., OS, Java version, Redis version).

### Suggesting Enhancements

We welcome suggestions for new features or improvements. Please submit an issue on GitHub with the following details:

- **Proposal**: A clear and concise description of the proposed enhancement.
- **Use Case**: The motivation behind the proposal and how it benefits users.
- **Alternatives Considered**: Any alternative solutions or features you've considered.

### Submitting Pull Requests

To contribute code:

1. **Fork the Repository**: Create your own fork of the repository.
2. **Create a Branch**: Make a new branch for your feature or bugfix.
3. **Commit Your Changes**: Commit your changes with clear and descriptive messages.
4. **Push to Branch**: Push your changes to your fork.
5. **Submit a Pull Request**: Open a pull request with a detailed description of your changes.

Please ensure your pull request adheres to the project's coding standards and includes tests for new functionality.

## Development Guidelines

### Setting Up the Development Environment

To set up your development environment:

1. **Clone the Repository**: Clone the repository to your local machine.
2. **Install Dependencies**: Ensure you have Java 11 or higher and Gradle installed.
3. **Build the Project**: Run `./gradlew build` to build the project.

### Coding Standards

- **Language**: The project is written in Kotlin.
- **Style Guide**: Follow the official [Kotlin coding conventions](https://kotlinlang.org/docs/coding-conventions.html).
- **Documentation**: Document public classes and methods with KDoc.

### Testing

- **Unit Tests**: Write unit tests for new features and bug fixes.
- **Test Framework**: Use JUnit 5 for writing tests.
- **Running Tests**: Execute `./gradlew test` to run the test suite.

## License

By contributing to RedPulsar, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
