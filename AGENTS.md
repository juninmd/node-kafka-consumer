```markdown
# AGENTS.md Guidelines

These guidelines outline the standards for development and maintenance of the AGENTS repository. Adherence to these principles is mandatory for all development activities.

## 1. DRY (Don't Repeat Yourself)

*   Avoid redundant code. Implement reusable components and functions whenever possible.
*   Refactor existing code to eliminate duplication.
*   Standardize naming conventions across modules and components.
*   Document functions and classes thoroughly, explaining their purpose and inputs/outputs.

## 2. KISS (Keep It Simple, Stupid)

*   Prioritize clarity and readability.
*   Strive for the shortest possible solution that meets the requirements.
*   Avoid unnecessary complexity.
*   Make code easy to understand for both the developer and others.

## 3. SOLID Principles

*   **Single Responsibility Principle:** Each class/component should have one, and only one, reason to change.
*   **Open/Closed Principle:** Systems should be open for extension but closed for modification.
*   **Liskov Substitution Principle:** Subclasses should be substitutable for their base classes without altering the correctness of the program.
*   **Interface Segregation Principle:** Clients shouldn't be forced to depend on methods they don't use.
*   **Dependency Inversion Principle:** High-level modules should not depend on low-level modules.  Instead, they should depend on abstractions.

## 4. YAGNI (You Aren't Gonna Need It)

*   Only implement features that are absolutely necessary at the time of development.
*   Don’t add features just because they might be useful in the future.
*   Defer implementation of features until they are explicitly required.

## 5. Code Length Constraint (180 Lines Max)

*   All files must be no more than 180 lines of code.
*   Include comments explaining complex logic or non-obvious code sections.

## 6. Test Coverage Requirements (80%)

*   All code must pass at least 80% of the automated tests.
*   Prioritize comprehensive testing of core functionality and critical components.
*   Ensure tests cover all relevant scenarios and edge cases.
*   Maintain a clear test suite with well-defined test cases for each module and component.

## 7. File Structure & Conventions

*   **Modules:** Organize code into logical modules with well-defined responsibilities.
*   **Classes:** Use descriptive class names that clearly indicate their purpose.
*   **Functions:**  Keep functions short and focused on a single task.
*   **Variables:** Use descriptive variable names.
*   **Documentation:** Provide JSDoc-style comments to document code functionality, inputs, and outputs.

## 8.  Development Process

*   Follow a consistent code review process.
*   Conduct regular code refactoring to improve code quality.
*   Use version control (Git) and maintain a branching strategy.
*   Document API endpoints using OpenAPI/Swagger.

## 9.  Specific Considerations for AGENTS (Example)**

*   All AGENT classes should have a clear `public` interface for interaction.
*   Data structures (dictionaries, lists) should be designed for efficient retrieval and manipulation.
*   Error handling should be robust and provide informative error messages.
*   Logging should be integrated throughout the code to aid in debugging and monitoring.

## 10.  Testing Practices

*   Use a unit testing framework (e.g., Jest, Mocha, Chai).
*   Write unit tests for all critical functions and components.
*   Implement integration tests to verify interactions between different AGENT components.
*   Ensure tests are easily runnable and maintainable.

## 11.  Code Style & Formatting

*   Use a consistent code style (e.g., ESLint, Prettier).
*   Follow a standardized formatting rule (e.g., Black, Docz).
*   Ensure code is easily parsed and understood.

## 12.  Documentation Requirements

*   Each module/class/function should have a concise docstring explaining its purpose.
*   API documentation should be readily accessible.

These guidelines are intended to promote a high-quality, maintainable, and efficient development process for the AGENTS repository.
```