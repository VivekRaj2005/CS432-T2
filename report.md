# CS 432 Assignment 3 Report

# Logical Dashboard & Transactional Validation

## Github Repository Link
[Link to Repository](https://github.com/VivekRaj2005/CS432-T2/new/main)

## Video Link
https://drive.google.com/drive/folders/1hOqmRwZBiZH3qI5N_KybBeNTaV_DvQRy?usp=sharing

We have a created a Graphical User Interface using JS. We have created seperate tabs for the CRUD operation and given a input field where the user can easily enter their input.

We have included a ACID test button to run the tests.

#### ACID Tests

Atomicity: Verifies that every operation either lands in both SQL and MongoDB or neither. Tests create/update/delete against both backends individually after the pipeline drains, and confirm that rejected operations (empty payload, empty set) leave both backends byte-for-byte unchanged.
Consistency: Verifies the system never enters an invalid state. The merged view must contain the union of what SQL and MongoDB each hold individually. Updates must not corrupt untouched fields. Sequential updates must converge to the final value in both backends, not stall on an intermediate one. Deletes must leave no phantom records in any source.
Isolation: Verifies concurrent transactions don't bleed into each other. Ten simultaneous creates must each produce exactly one distinct record with no collisions. Two concurrent updates to different records must not cross-contaminate. Readers running alongside writers must never observe a partially written record (some fields present, others missing). Two concurrent updates to the same record must not produce a split-brain where SQL and MongoDB disagree on the final value.
Durability: Verifies committed data survives beyond the process. A record fully flushed to both backends before a dump must still be there after a reload — this rules out durability that is only queue-deep. Committed updates must not be rolled back to the old value by a reload. Committed deletes must not be resurrected. Three consecutive dump/load cycles must produce an identical MapRegister each time, ruling out cumulative drift. The restart test (opt-in via IITGNDB_MAIN_CMD) sends SIGTERM, restarts the process, and confirms both backends still hold the record.


## Contribution
- Harinarayan J: ACID Testing
- Vivek Raj: Front End Development
