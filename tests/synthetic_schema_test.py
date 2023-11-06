from flatfile_mapping.synthetic_schema import combine_fields, schema_from_field_names


def to_json(schemas):
    return [schema.model_dump(mode="json", by_alias=True) for schema in schemas]


def test_construct_nested_schema_from_field_names():
    field_names = [
        "name",
        "math.score.2001",
        "math.score.2002",
        "english.score.2001",
        "english.score.2002",
    ]

    schema = schema_from_field_names(field_names)

    fields = sorted(schema.fields, key=lambda f: f.key)

    assert to_json(fields) == [
        {
            "type": "array",
            "key": "english.score",
            "base": {
                "type": "schema",
                "schema": {
                    "fields": [
                        {"key": "english.score", "type": "string"},
                        {"key": "__control", "type": "string"},
                    ]
                },
            },
        },
        {
            "type": "array",
            "key": "math.score",
            "base": {
                "type": "schema",
                "schema": {
                    "fields": [
                        {"key": "math.score", "type": "string"},
                        {"key": "__control", "type": "string"},
                    ]
                },
            },
        },
        {"key": "name", "type": "string"},
    ]


def test_combine_nested_schema():
    field_names = [
        "name",
        "math.score.2001",
        "math.score.2002",
        "english.score.2001",
        "english.score.2002",
    ]

    schema = schema_from_field_names(field_names)
    combined = combine_fields(schema, "scores", "([a-z]+).score")

    assert to_json(combined.fields) == [
        {"type": "string", "key": "name"},
        {
            "type": "array",
            "key": "scores",
            "base": {
                "type": "schema",
                "schema": {
                    "fields": [
                        {"key": "__control", "type": "string"},
                        {"key": "math", "type": "string"},
                        {"key": "english", "type": "string"},
                    ]
                },
            },
        },
    ]
