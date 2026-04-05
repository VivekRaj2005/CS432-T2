# IITGnDB User Manual

## Overview

IITGnDB is a web UI for creating, fetching, updating, deleting, and inspecting records in a mixed SQL and MongoDB-backed system. Use the top navigation bar to move between the main screens.

## Navigation

The navbar includes these pages:

- Create
- Get
- Update
- Delete
- View Schema
- Dump/Load

## Create

Use the Create page to add a new record.

### Fields

- Field Name: the key to store in the record.
- Type: choose the data type.
- Input: enter the value based on the selected type.

### Supported types

- `string`
- `number`
- `boolean`
- `dict`
- `list<string>`
- `list<number>`
- `list<boolean>`

### Nested input

If you choose `dict`, the form expands into a nested builder. Each nested field has the same controls as the root field, so you can create deeper structures without typing raw JSON.

### Notes

- `list` types use multiple item inputs.
- Boolean fields use dropdowns.
- Number fields validate numeric input.

## Get

Use the Get page to fetch records.

### Options

- Source: select `merged`, `sql`, or `nosql`.
- Limit: maximum number of rows to display.
- Conditions: add one or more filter rows.

### Filters

You can filter by:

- equals and not equals
- greater than / less than
- length comparisons
- array membership operations
- dict key/value checks

### Display

Results are shown in a table. The UI hides `table_autogen_id` and foreign-key reference columns from normal display. Arrays and dicts are displayed with compact previews.

## Update

Use the Update page to modify records.

### How it works

- Provide criteria to identify the row.
- Provide the fields to update.
- You can use explicit criteria or inferred identifier fields.

### Notes

- Nested field updates are supported where applicable.
- The system splits updates by SQL and NoSQL storage automatically.

## Delete

Use the Delete page to remove records.

### How it works

- Provide criteria or a matching filter.
- Choose the source when needed.

### Notes

- Deleting a record also removes related nested child rows.
- Storage recalculation occurs after delete operations.

## View Schema

Use the Schema page to inspect the current schema state.

### What you will see

- Field metadata
- Field classifications
- Foreign key references
- Nested schema tables

### Notes

- Nested fields are shown as nested schema, not flattened raw keys.
- Foreign keys are shown as reference-only links.
- Storage assignment may be SQL or NoSQL depending on classifier output.

## Dump / Load

Use the Dump/Load page to save or restore runtime state.

### Download Runtime Dump

- Downloads the live runtime snapshot as a JSON file.
- No server-side path is required.

### Load From File

- Upload a JSON dump file from your machine.
- The app accepts both runtime dump files and dummy data dumps.

### Supported dump formats

- Runtime dump: contains `map_register`, queue state, and classifier state.
- Data dump: contains `data` as a list of records to queue for ingestion.

## Dummy Dump Generator

The project includes a helper script at `test/generateDumpFile.py` to generate sample JSON dumps.

It creates dummy records with:

- `username` as a unique value
- `grade`
- `marks`
- `prof` with nested `name` and `dept`
- `Course Codes` as a list of strings
- `Clubs` as a string or `null`

Example:

```bash
python test/generateDumpFile.py --count 200 --output test/dummy_dump.json --seed 432 --clubs-null-rate 0.35
```

## Recommended Workflow

1. Create or load data.
2. Inspect schema if needed.
3. Fetch records with filters to verify the result.
4. Download a runtime dump if you want a full snapshot.
5. Upload a dump later to restore or replay the data.

## Troubleshooting

- If the app shows an upload error, verify the file is valid JSON.
- If the dump file does not restore, ensure it is either a runtime dump or a data dump with a `data` array.
- If results look empty, check the selected source and the active filters.
- If the frontend cannot reach the API, make sure the backend server is running on the expected address.
