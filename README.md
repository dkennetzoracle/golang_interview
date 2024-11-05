# Simple Database Challenge

In the Simple Database problem, you'll implement an in-memory key/value
database similar to Redis. For simplicity's sake, instead of dealing with IO
you only need to implement the internal functionality as if it were a database
library.

## Guidelines

* You have 25 minutes to work on the implementation. Get as far as you can and
  be prepared to discuss your approach and how you would complete the exercise
  afterward.
 
## Data Commands

The database accepts the following commands to operate on keys:

* `SET name value` – Set the variable `name` to the value `value`. For
  simplicity `value` may be an integer.
* `GET name` – Print out the value of the variable `name`, or `NULL` if that
  variable is not set.
* `UNSET name` – Unset the variable `name`, making it just like that variable
  was never set.

Any data command that is run outside of a transaction should commit
immediately. Here are some example command sequences:

### Unset a non-existant value returns an error
|INPUT|OUTPUT|
|:----|:-----|
|UNSET a| ERROR|

### Get a non-existant value returns no error (nil)
|INPUT|OUTPUT|
|:----|:-----|
| GET a | NULL |

### Set, Get, Unset, Get
|INPUT|OUTPUT|
|:----|:-----|
|SET ex 10| |
|GET ex|10|
|UNSET ex||
|GET ex|NULL|

### Overwrite
|INPUT|OUTPUT|
|:----|:-----|
|SET b 10||
|SET b 30||
|GET b|30|


## Transaction Commands

In addition to the above data commands, your program should also support
database transactions by also implementing these commands:

* `BEGIN` – Open a new transaction block. **Transactions can be nested;** a
  `BEGIN` can be issued inside of an existing block.
* `ROLLBACK` – Undo commands issued in the current transaction, and closes it
  Returns an error if no transaction is in progress.
* `COMMIT` – Close **all** open transactions, permanently applying the changes
  made in them. Returns an error if no transaction is in progress. `COMMIT` on an empty transaction is a no-op, but not an error.

### Error on commit when no transaction is in progress
|INPUT|OUTPUT|
|:---|:---|
|COMMIT| ERROR|

### Ability to nest transactions
|INPUT|OUTPUT|
|:---|:---|
|BEGIN| |
|SET a 10| |
|GET a| 10|
|BEGIN|   |
|SET a 20|    |
|GET a|  20|
|ROLLBACK| |
|GET a |      10|
|ROLLBACK| |
|GET a | NULL |
|END| |

### Error if rollback with no transaction in progress
|INPUT| OUTPUT|
|:---|:-----|
|BEGIN||
|SET a 30||
|BEGIN||
|SET a 40||
|COMMIT||
|GET a |40|
|ROLLBACK| ERROR|
|END|

### Immediately commit outside of transaction. Modify values inside nested transactions. 
|INPUT|OUTPUT|
|:----|:-----|
|SET a 50||
|BEGIN||
|GET a|50|
|SET a 60||
|BEGIN||
|UNSET a||
|GET a| NULL|
|ROLLBACK||
|GET a| 60|
|COMMIT||
|GET a|60|
|END||
