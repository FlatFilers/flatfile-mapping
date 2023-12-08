# flatfile-mapping

This is a Python library for using Flatfile's mapping features. Here you can think of "mapping" as the process of
turning a source record into a target record. At its simplest this might just be mapping one field to another
(e.g. "first_name" to "firstName"). But it can also perform more complex transformations, such as
extracting a substring from a field or concatenating multiple fields together.

At a high level, the library consists of three parts:

1. A domain-specific language for defining mapping rules, as described in the [Mapping Rules](#mapping-rules) section.
2. A `MappingProgram` class that can be used to execute a "program" of mapping rules against either "records" or Pandas dataframes.
3. Code for interacting with the Flatfile mapping API to get AI-suggested sets of mapping rules for a given source and target schema.

# Mapping Rules

The mapping rules are defined using a domain-specific language (DSL) defined in the `flatfile_mapping.mapping_rule` module.

For illustration, imagine you have the following "records":

```python
records = [
    {"name": "Dave", "age": 42, "location": "San Francisco"},
    {"name": "Bob", "age": 32, "location": "San Francisco"},
    {"name": "Alice", "age": 22, "location": "New York"},
]
```

Mapping rules specify logic for using transforming each record into a new destination record.
In Python mapping rules are represented by Pydantic schemas and can be generated from JSON
using the `parse` function:

```python
from flatfile_mapping import parse
```

In what follows the mapping rules will be shown as their JSON representations.

Rules are intended to be applied in order, and the results of earlier rules
can be used by (or overwritten by) later rules.

## Assign

An _assign_ rule looks like

```python
{
    "type": "assign",
    "sourceField": "name",
    "destinationField": "nickname",
}
```

This rule says to populate the "nickname" field in the destination record with the value of the "name" field in the source record.

## Ignore

An _ignore_ rule does nothing. It looks like

```python
{
    "type": "ignore",
    "sourceField": "name",
}
```

This rule says to ignore the "name" field in the source record.
(Strictly speaking this rule is not necessary, since a field that is not used by any rule is de facto ignored,
but it exists as a record of an explicit decision to ignore a field.)

## Constant

A _constant_ rule sets a field to a constant value, it looks like:

```python
{
    "type": "constant",
    "destinationField": "iq,
    "value": 100,
}
```

## FindReplace

A _find-replace_ rule replaces substrings in a field with other substrings.
It operates on a destination field, so it should come after an "assign" rule or similar.

```python
{
    "type": "find-replace",
    "destinationField": "nickname",
    "values: [
      {"find": "Dave", "replace": "David"},
      {"find": "Bob", "replace": "Robert"},
    ]
}
```

## Transform

A _transform_ rule applied a precanned transformation to a field.
Currently the implemented transformations are "uppercase" and "lowercase".
It looks like

```python
{
    "type": "transform",
    "sourceField": "name",
    "destinationField": "nickname",
    "transform": "uppercase",
}
```

## RegexExtract

A _regex-extract_ rule extracts zero or more substrings from a field
using a regular expression. For instance, to extract the "Saint" from locations (e.g. "San Francisco" -> "Francisco") you could use:

```python
{
    "type": "regex-extract",
    "sourceField": "location",
    "destinationFields": ["saint"],
    "regex": "^San (.*)$",
},
```

## Interpolate

An _interpolate_ rule interpolates a string using values from other fields.

```python
{
    "type": "interpolate",
    "sourceFields": ["name"],
    "destinationField": "greeting",
    "template": "Hello, {0}!",
}
```

## Arithmetic

An _arithmetic_ rule computes a value using arithmetic operations on other fields.

```python
{
    "type": "arithmetic",
    "equation": "(age * 4) + 10",
    "sourceFields": ["age"],
    "destinationField": "iq",
}
```

## Delete

A _delete_ rule deletes a rule from the destination record.
You might use that if say you've created intermediate values there
that you don't want in the final output:

```python
{
    "type": "delete",
    "destinationField": "greeting",
}
```

## Subprogram

A _subprogram_ rule is a "grouping" of other rules.
This is most useful if you are using _filters_ (see below) to conditionally apply rules.

```python
{
    "type": "subprogram",
    "rules": [
        {
            "type": "assign",
            "sourceField": "name",
            "destinationField": "nickname",
        },
        {
            "type": "assign",
            "sourceField": "age",
            "destinationField": "howOld",
        },
    ],
}
```

## Coalesce

A _coalesce_ rule takes a list of fields and returns the first non-None value:

```python
{
    "type": "coalesce",
    "sourceFields": ["name", "location"],
    "destinationField": "nickname",
}
```

Here the nickname will be the name if it is not None, otherwise it will be the location.

## Concatenate

A _concatenate_ rule concatenates the values of multiple fields:

```python
{
    "type": "concatenate",
    "sourceFields": ["name", "location"],
    "destinationField": "nickname",
    "separator": " of "
}
```

If you don't specify the separator, the default is ",".

## Array

The _array_ rule is similar to the _concatenate_ rule, but instead of concatenating the values of multiple fields, it creates an array of those values:

```python
{
    "type": "array",
    "sourceFields": ["name", "location"],
    "destinationField": "nickname",
}
```

would result in an output record that looks like

```python
{
    "nickname": ["Dave", "San Francisco"],
}
```

This and the _nest_ rule are more complicated in that they create more complex nested records.

## Nest

The _nest_ rule is more complicated than the other rules. It is designed for the case
where your source record has "columns" that represent some kind of nested structure.

### Simple Nesting

For example, imagine that you start with the following source records,
and that you want to create a destination record with a single nested "address" field.

```python
records = [
    {"name": "Dave", "address1": "123 Main St", "address2": "Apt 1"},
    {"name": "Bob", "address1": "456 Main St", "address2": "Apt 2"},
    {"name": "Alice", "address1": "789 Main St", "address2": "Apt 3"},
]
```

You could define the following rule:

```python
{
    "type": "nest",
    "subfields": [
        {
            "sourceRegex": "address([0-9]+)",
            "destinationSubfield": "value",
        },
    ],
    "destinationField": "address",
}
```

This means a couple of things. First, the "sourceRegex" field tells us
which fields we're looking for in the source record. In this case
we're looking for fields that start with "address" and end with a number.

It also contains a capture group around the number, which means that the
number will be extracted and used as the "\_\_control" of the nested subrecord.

The "destinationSubfield" tells us where to put the value of the field in each nested subrecord.

This rule (combined with an _assign_ rule for name) would result in the following destination records:

```python
{
    "name": "Dave",
    "address": [
        {"value": "123 Main St", "__control": "1"},
        {"value": "Apt 1", "__control": "2"},
    ],
},
{
    "name": "Bob",
    "address": [
        {"value": "456 Main St", "__control": "1"},
        {"value": "Apt 2", "__control": "2"},
    ],
},
{
    "name": "Alice",
    "address": [
        {"value": "789 Main St", "__control": "1"},
        {"value": "Apt 3", "__control": "2"},
    ],
}
```

### More Complex Nesting

You can also nest multiple fields at once. For example, consider the following records:

```python
records = [
    {
        "name": "Dave",
        "math.score.1": 10,
        "math.score.2": 20,
        "english.score.1": 30,
        "english.score.2": 40,
    },
    {
        "name": "Bob",
        "math.score.1": 50,
        "math.score.2": 60,
        "english.score.1": 70,
        "english.score.2": 80,
    },
]
```

Here we have two subjects (math and english) and two scores for each subject.

By providing multiple "subfields" in the mapping rule, we can
create nested subrecords with both "math" and "english" fields:

```python
{
    "type": "nest",
    "subfields": [
        {
            "sourceRegex": "math.score.([0-9]+)",
            "destinationSubfield": "math",
        },
        {
            "sourceRegex": "english.score.([0-9]+)",
            "destinationSubfield": "english",
        },
    ],
    "destinationField": "scores",
}
```

Again, with an assign rule for "name", this would result in the following destination records:

```python
{
    "name": "Dave",
    "scores": [
        {"math": "10", "english": "30", "__control": "1"},
        {"math": "20", "english": "40", "__control": "2"},
    ],
},
{
    "name": "Bob",
    "scores": [
        {"math": "50", "english": "70", "__control": "1"},
        {"math": "60", "english": "80", "__control": "2"},
    ],
}
```

## source! and destination!

In general if a rule has a `sourceField` it will look for it in the source record,
and if it has a `destinationField` it will look for it in the destination record.

But sometimes you want to use a field from the destination record in a rule that
normally does something to a field in the source record. For example, you might
want to use the destination record's "name" field to create a "greeting" field.

By convention, if a source field starts with "destination!" it will be looked up in the
destination record instead of the source record. So you could do something like:

```python
{
    "type": "interpolate",
    "sourceFields": ["destination!name"], # <--- note the "destination!" prefix
    "destinationField": "greeting",
    "template": "Hello, {0}!",
}
```

You can do the opposite too and assign a value to a field in the source record
by supplying a "destination field" that starts with "source!", but I don't
recommend you do this.

## Filters

Every rule can have a "filter" field, which is an [FFQL](https://flatfile.com/docs/developer-tools/flatfile_query_language) expression indicating which records the rule should be applied to.

For example, imagine you want to assign the "name" field to the "nickname" field,
except if the name starts with D, in which case you want the nickname to be "D Dawg":

```python
{
    "type": "assign",
    "sourceField": "name",
    "destinationField": "nickname",
},
{
    "type": "constant",
    "destinationField": "nickname",
    "value": "D Dawg",
    "filter": "name like D%"
}
```

Check out the FFQL documentation to see what all it can do.

# Mapping Programs

A mapping program is simply a collection of mapping rules
that can be applied to a record or pandas dataframe.

You can instantiate it from rules or from their JSON representation.

```python
from flatfile_mapping import MappingProgram

mapping_program = MappingProgram.from_json([
    {
        "type": "assign",
        "sourceField": "name",
        "destinationField": "nickname",
    },
    {
        "type": "assign",
        "sourceField": "age",
        "destinationField": "howOld",
    },
])
```

## Applied to Records

```python
records = [
    {"name": "Dave", "age": 42, "location": "San Francisco"},
    {"name": "Bob", "age": 32, "location": "San Francisco"},
    {"name": "Alice", "age": 22, "location": "New York"},
]

mapped_records = mapping_program.run(records)
```

Which will result in the following records:

```python
[
    {"nickname": "Dave", "howOld": 42},
    {"nickname": "Bob", "howOld": 32},
    {"nickname": "Alice", "howOld": 22},
]
```

## Applied to Dataframes

```python
import pandas as pd

records_df = pd.DataFrame(
    [
        {"name": "Dave", "age": 42, "location": "San Francisco"},
        {"name": "Bob", "age": 32, "location": "San Francisco"},
        {"name": "Alice", "age": 22, "location": "New York"},
    ]
)

mapped_records_df = mapping_program.run_df(records_df)
```

Which will result in the following dataframe:

```python
  nickname  howOld
0     Dave      42
1      Bob      32
2    Alice      22
```

# Interacting with the Flatfile API

If you have a Flatfile API key, you can use the `flatfile_mapping.automapping` module
to get AI-suggested mapping rules for a given source and target schema.

```python
from flatfile_mapping.automapping import get_mapping_rules

rules = get_mapping_rules(
  source_fields=["first", "middle", "last", "zip code"],
  destination_fields=["firstName", "lastName", "postal"],
  api_key="my-api-key"
)
```

This will (as of this writing) return the following rules (these are the Pydantic representations):

```python
[
  Assign(
    name='Alias "lastName" as "last"',
    type='assign',
    sourceField='last',
    destinationField='lastName'
  ),
  Assign(
    name='Alias "firstName" as "first"',
    type='assign',
    sourceField='first',
    destinationField='firstName'
  ),
  Assign(
    name='Alias "postal" as "zip code"',
    type='assign',
    sourceField='zip code',
    destinationField='postal'
  ),
  Ignore(
    name='Ignore "middle"',
    type='ignore',
    sourceField='middle'
  )
]
```

Behind the scenes, the Flatfile API is using a machine learning model to compute
similarities between your source and destination fields and then using those
similarities to find the "best" assignment of source fields to destination fields.

Currently this model only suggests _assign_ and _ignore_ rules. In the future it
will suggest more types of rules.

By default it only suggests matches that have a similarity score of 0.5 or higher.
This is probably the value you want, but you can specify a different threshhold
by providing a `mapping_confidence_threshold` argument to `get_mapping_rules`.

# Installation

```bash
pip install flatfile-mapping
```
