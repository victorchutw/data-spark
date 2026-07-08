# Triage Labels

The skills speak in terms of five canonical triage roles. This file maps those roles to the actual label strings used in this repo's issue tracker.

| Label in mattpocock/skills | Label in our tracker | Meaning                                  |
| -------------------------- | -------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            |
| `wontfix`                  | `wontfix`            | Will not be actioned                     |

When a skill mentions a role, use the corresponding label string from this
table. Do not create alternate labels with similar meanings.

For each open issue or external PR in the triage queue, keep exactly one
canonical triage label unless the item has moved into one of the execution
states below.

## Execution Labels

GitOps work also uses these additional state labels. Each open issue or external
PR should have only one state label from this table or the canonical triage
table above.

| Label | Meaning |
| ----- | ------- |
| `in-progress` | Someone or an agent is actively working on this issue |
| `in-review` | A PR is open and ready for review |
| `blocked` | Work cannot continue until a named blocker is resolved |
