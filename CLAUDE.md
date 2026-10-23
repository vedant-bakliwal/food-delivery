# Notes for AI assistants working in this repo

- Build and test: `mvn -B verify`. AWS tests (`AwsMotoTest`) need Docker; they skip without it.
- `core/` must stay free of Spring and AWS imports. Add behaviour there and test it with the in-memory adapters first.
- Every new failure-handling behaviour needs a row in the README table and a test that fails without it.
- Event JSON is a contract. Changing a field means updating `src/test/resources/contracts` in the same change and saying why in the PR.
- Keep handlers idempotent: check order status and version before changing anything, and treat "already done" as success.
- All data is invented. Do not add real company, restaurant or customer data.
